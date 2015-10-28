/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tephra;

import co.cask.tephra.persist.TransactionEdit;
import co.cask.tephra.persist.TransactionLogReader;
import com.google.common.io.Closeables;
import com.google.common.primitives.Bytes;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * {@link TransactionLogReader} that can read v2 version of Transaction logs. The logs are expected to
 * have a sequence of {@link TransactionEdit}s along with commit markers that indicates the size of the batch of
 * {@link TransactionEdit}s that were synced together. If the expected number of {@link TransactionEdit}s are not
 * present then that set of {@link TransactionEdit}s are discarded.
 */
public class HDFSTransactionLogReaderV2 extends AbstractHDFSLogReader {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSTransactionLogReaderV2.class);

  private final Queue<TransactionEdit> transactionEdits;
  private final LongWritable key;

  public HDFSTransactionLogReaderV2(SequenceFile.Reader reader) {
    super(reader);
    this.transactionEdits = new ArrayDeque<>();
    this.key = new LongWritable();
  }

  @Override
  public TransactionEdit next() throws IOException {
    return next(null);
  }

  @Override
  public TransactionEdit next(TransactionEdit reuse) throws IOException {
    if (isClosed()) {
      return null;
    }

    if (transactionEdits.size() != 0) {
      return transactionEdits.remove();
    }

    // Read edits until we reach the commit marker
    populateTransactionEdits(reader, transactionEdits);
    return transactionEdits.poll();
  }

  private void populateTransactionEdits(SequenceFile.Reader reader, Queue<TransactionEdit> transactionEdits)
    throws IOException {
    boolean success;
    DataOutputBuffer rawKey = new DataOutputBuffer();

    // Check if we are at the end of file
    if (reader.nextRawKey(rawKey) != -1) {
      byte[] keyBytes = TxConstants.TransactionLog.NUM_ENTRIES_APPENDED.getBytes();
      // Check if we are at the commit marker
      if (rawKey.getLength() == keyBytes.length && Bytes.indexOf(rawKey.getData(), keyBytes) != -1) {
        int numEntries;
        SequenceFile.ValueBytes valueBytes = reader.createValueBytes();
        Closeables.closeQuietly(rawKey);

        // commit marker key matched, read the data now to determine numEntries to read.
        reader.nextRawValue(valueBytes);
        ByteArrayOutputStream value = new ByteArrayOutputStream(Integer.SIZE);
        DataOutputStream outputStream = new DataOutputStream(value);
        valueBytes.writeUncompressedBytes(outputStream);
        outputStream.flush();
        Closeables.closeQuietly(outputStream);

        numEntries = ByteBuffer.wrap(value.toByteArray()).getInt();
        Closeables.closeQuietly(value);

        for (int i = 0; i < numEntries; i++) {
          TransactionEdit edit = new TransactionEdit();
          try {
            success = reader.next(key, edit);
            if (success) {
              transactionEdits.add(edit);
            } else {
              throw new EOFException("Attempt to read TransactionEdit failed.");
            }
          } catch (EOFException e) {
            // we have reached EOF before reading back numEntries, we clear the partial list and return.
            LOG.warn("Reached EOF in log before reading {} entries. Ignoring all {} edits since the last marker",
                     numEntries, transactionEdits.size(), e);
            transactionEdits.clear();
          }
        }
      } else {
        LOG.error("Invalid key for num entries appended found, expected : {}",
                  TxConstants.TransactionLog.NUM_ENTRIES_APPENDED);
      }
    }
  }
}
