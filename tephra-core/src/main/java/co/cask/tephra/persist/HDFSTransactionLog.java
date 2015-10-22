/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.tephra.persist;

import co.cask.tephra.TxConstants;
import co.cask.tephra.metrics.MetricsCollector;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Allows reading from and writing to a transaction write-ahead log stored in HDFS.
 */
public class HDFSTransactionLog extends AbstractTransactionLog {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSTransactionLog.class);

  private final FileSystem fs;
  private final Configuration hConf;
  private final Path logPath;

  /**
   * Creates a new HDFS-backed write-ahead log for storing transaction state.
   * @param fs Open FileSystem instance for opening log files in HDFS.
   * @param hConf HDFS cluster configuration.
   * @param logPath Path to the log file.
   */
  public HDFSTransactionLog(final FileSystem fs, final Configuration hConf,
                            final Path logPath, long timestamp, MetricsCollector metricsCollector) {
    super(timestamp, metricsCollector);
    this.fs = fs;
    this.hConf = hConf;
    this.logPath = logPath;
  }

  @Override
  protected TransactionLogWriter createWriter() throws IOException {
    return new LogWriter(fs, hConf, logPath);
  }

  @Override
  public String getName() {
    return logPath.getName();
  }

  @Override
  public TransactionLogReader getReader() throws IOException {
    FileStatus status = fs.getFileStatus(logPath);
    long length = status.getLen();

    LogReader reader = null;
    // check if this file needs to be recovered due to failure
    // Check for possibly empty file. With appends, currently Hadoop reports a
    // zero length even if the file has been sync'd. Revisit if HDFS-376 or
    // HDFS-878 is committed.
    if (length <= 0) {
      LOG.warn("File " + logPath + " might be still open, length is 0");
    }

    try {
      HDFSUtil hdfsUtil = new HDFSUtil();
      hdfsUtil.recoverFileLease(fs, logPath, hConf);
      try {
        FileStatus newStatus = fs.getFileStatus(logPath);
        LOG.info("New file size for " + logPath + " is " + newStatus.getLen());
        SequenceFile.Reader fileReader = new SequenceFile.Reader(fs, logPath, hConf);
        reader = new LogReader(fileReader);
      } catch (EOFException e) {
        if (length <= 0) {
          // TODO should we ignore an empty, not-last log file if skip.errors
          // is false? Either way, the caller should decide what to do. E.g.
          // ignore if this is the last log in sequence.
          // TODO is this scenario still possible if the log has been
          // recovered (i.e. closed)
          LOG.warn("Could not open " + logPath + " for reading. File is empty", e);
          return null;
        } else {
          // EOFException being ignored
          return null;
        }
      }
    } catch (IOException e) {
      throw e;
    }

    return reader;
  }

  @VisibleForTesting
  static final class LogWriter implements TransactionLogWriter {
    private final SequenceFile.Writer internalWriter;
    private List<Entry> transactionEntries;
    public LogWriter(FileSystem fs, Configuration hConf, Path logPath) throws IOException {
      // TODO: retry a few times to ride over transient failures?
      SequenceFile.Metadata metadata = new SequenceFile.Metadata();
      metadata.set(new Text("version"), new Text("v2"));

      this.internalWriter =
        SequenceFile.createWriter(fs, hConf, logPath, LongWritable.class,
                                  TransactionEdit.class, SequenceFile.CompressionType.NONE, null, null, metadata);
      transactionEntries = new ArrayList<>();

      LOG.debug("Created a new TransactionLog writer for " + logPath);
    }

    @Override
    public void append(Entry entry) throws IOException {
      transactionEntries.add(entry);
    }

    @Override
    public void sync() throws IOException {
      // write the number of entries we are writing to the log
      if (transactionEntries.size() > 0) {
        String key = TxConstants.TransactionLog.NUM_ENTRIES_APPENDED;
        internalWriter.appendRaw(key.getBytes(), 0, key.getBytes().length,
                                 new NumEntriesBytes(transactionEntries.size()));
      }

      // write the entries to the log
      for (Entry entry : transactionEntries) {
        internalWriter.append(entry.getKey(), entry.getEdit());
      }
      transactionEntries.clear();
      internalWriter.syncFs();
    }

    @Override
    public void close() throws IOException {
      transactionEntries.clear();
      internalWriter.close();
    }
  }

  private static final class NumEntriesBytes implements SequenceFile.ValueBytes {
    private final int numEntries;

    public NumEntriesBytes(int numEntries) {
      this.numEntries = numEntries;
    }

    @Override
    public void writeUncompressedBytes(DataOutputStream outStream) throws IOException {
      outStream.write(Ints.toByteArray(numEntries));
    }

    @Override
    public void writeCompressedBytes(DataOutputStream outStream) throws IllegalArgumentException, IOException {
      throw new IllegalArgumentException("UncompressedBytes cannot be compressed!");
    }

    @Override
    public int getSize() {
      return Ints.BYTES;
    }
  }

  private static final class LogReader implements TransactionLogReader {
    private boolean closed;
    private SequenceFile.Reader reader;
    private LongWritable key = new LongWritable();
    private boolean newVersion;
    private List<TransactionEdit> transactionEdits;

    public LogReader(SequenceFile.Reader reader) {
      this.reader = reader;
      newVersion = reader.getMetadata().getMetadata().containsKey(new Text("version"));
      transactionEdits = new ArrayList<>();
    }

    @Override
    public TransactionEdit next() {
      try {
        if (newVersion) {
          return next(null);
        }
        return next(new TransactionEdit());
      } catch (IOException ioe) {
        throw Throwables.propagate(ioe);
      }
    }

    @Override
    public TransactionEdit next(TransactionEdit reuse) throws IOException {
      if (closed) {
        return null;
      }
      if (transactionEdits.size() != 0) {
        return transactionEdits.remove(0);
      } else {
        if (newVersion) {
          // v2 format, buffer edits till we reach a marker
          boolean successful;
          DataOutputBuffer rawKey = new DataOutputBuffer();

          if (reader.nextRawKey(rawKey) != -1) {
            // has data and has not reached EOF
            byte [] keyBytes = TxConstants.TransactionLog.NUM_ENTRIES_APPENDED.getBytes();
            if (rawKey.getLength() == keyBytes.length && Bytes.indexOf(rawKey.getData(), keyBytes) != -1) {
              int numEntries;
              SequenceFile.ValueBytes valueBytes = reader.createValueBytes();
              Closeables.closeQuietly(rawKey);

              // key matched, read the data now to determine numEntries to read.
              reader.nextRawValue(valueBytes);
              ByteArrayOutputStream value = new ByteArrayOutputStream(Integer.SIZE);
              DataOutputStream outputStream = new DataOutputStream(value);
              valueBytes.writeUncompressedBytes(outputStream);
              outputStream.flush();
              Closeables.closeQuietly(outputStream);
              numEntries = ByteBuffer.wrap(value.toByteArray()).getInt();
              Closeables.closeQuietly(value);

              // read numEntries into the list
              for (int i = 0; i < numEntries; i++) {
                TransactionEdit edit = new TransactionEdit();
                try {
                  successful = reader.next(key, edit);
                  if (successful) {
                    transactionEdits.add(edit);
                  } else {
                    transactionEdits.clear();
                    return null;
                  }
                } catch (EOFException e) {
                  // if we have reached EOF before reading back the numEntries, we clear the partial list and return.
                  transactionEdits.clear();
                  return null;
                }
              }
            } else {
              LOG.error("Invalid key for num entries appended found, expected : {}",
                        TxConstants.TransactionLog.NUM_ENTRIES_APPENDED);
            }
          }

          if (!transactionEdits.isEmpty()) {
            return transactionEdits.remove(0);
          } else {
            return null;
          }

        } else {
          try {
            // does not have marker, version v1.
            boolean successful = reader.next(key, reuse);
            return successful ? reuse : null;
          } catch (EOFException e) {
            LOG.warn("Hit an unexpected EOF while trying to read the Transaction Edit. Skipping the entry. {}", e);
            return null;
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      reader.close();
      closed = true;
    }
  }
}
