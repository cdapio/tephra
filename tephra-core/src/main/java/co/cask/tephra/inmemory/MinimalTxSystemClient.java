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

package co.cask.tephra.inmemory;

import co.cask.tephra.InvalidTruncateTimeException;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionCouldNotTakeSnapshotException;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TransactionType;
import co.cask.tephra.TxConstants;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;

/**
 * Dummy implementation of TxSystemClient. May be useful for perf testing.
 */
public class MinimalTxSystemClient implements TransactionSystemClient {
  private long currentTxPointer = 1;

  @Override
  public Transaction startShort() {
    long wp = currentTxPointer++;
    // NOTE: -1 here is because we have logic that uses (readpointer + 1) as a "exclusive stop key" in some datasets
    return new Transaction(
      Long.MAX_VALUE - 1, wp, new long[0], new long[0],
      Transaction.NO_TX_IN_PROGRESS, TransactionType.SHORT);
  }

  @Override
  public Transaction startShort(int timeout) {
    return startShort();
  }

  @Override
  public Transaction startLong() {
    return startShort();
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    return true;
  }

  @Override
  public boolean commit(Transaction tx) {
    return true;
  }

  @Override
  public void abort(Transaction tx) {
    // do nothing
  }

  @Override
  public boolean invalidate(long tx) {
    return true;
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    throw new TransactionCouldNotTakeSnapshotException("Not snapshot to take.");
  }

  @Override
  public String status() {
    return TxConstants.STATUS_OK;
  }

  @Override
  public void resetState() {
    // do nothing
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    return true;
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    return true;
  }

  @Override
  public int getInvalidSize() {
    return 0;
  }
}
