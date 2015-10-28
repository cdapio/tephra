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

import co.cask.tephra.persist.TransactionLogReader;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

/**
 * Abstract class for HDFS {@link TransactionLogReader}s.
 */
public abstract class AbstractHDFSLogReader implements TransactionLogReader {
  private boolean closed;

  protected final SequenceFile.Reader reader;

  public AbstractHDFSLogReader(SequenceFile.Reader reader) {
    this.reader = reader;
  }

  protected boolean isClosed() {
    return closed;
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
