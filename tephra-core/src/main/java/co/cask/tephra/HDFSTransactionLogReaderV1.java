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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;

/**
 * {@link TransactionLogReader} that can read v1 (default) version of Transaction logs. The logs are expected to
 * have a sequence of {@link TransactionEdit}s.
 */
public class HDFSTransactionLogReaderV1 extends AbstractHDFSLogReader {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSTransactionLogReaderV1.class);
  private final LongWritable key;

  public HDFSTransactionLogReaderV1(SequenceFile.Reader reader) {
    super(reader);
    this.key = new LongWritable();
  }

  @Override
  public TransactionEdit next() throws IOException {
    return next(new TransactionEdit());
  }

  @Override
  public TransactionEdit next(TransactionEdit reuse) throws IOException {
    if (isClosed()) {
      return null;
    }

    try {
      boolean successful = reader.next(key, reuse);
      return successful ? reuse : null;
    } catch (EOFException e) {
      LOG.warn("Hit an unexpected EOF while trying to read the Transaction Edit. Skipping the entry.", e);
      return null;
    }
  }
}
