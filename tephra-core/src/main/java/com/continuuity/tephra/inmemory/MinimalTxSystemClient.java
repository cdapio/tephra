/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.tephra.inmemory;

import com.continuuity.tephra.TransactionCouldNotTakeSnapshotException;
import com.continuuity.tephra.TransactionSystemClient;
import com.continuuity.tephra.TxConstants;

import java.io.InputStream;
import java.util.Collection;

/**
 * Dummy implementation of TxSystemClient. May be useful for perf testing.
 */
public class MinimalTxSystemClient implements TransactionSystemClient {
  private long currentTxPointer = 1;

  @Override
  public com.continuuity.tephra.Transaction startShort() {
    long wp = currentTxPointer++;
    // NOTE: -1 here is because we have logic that uses (readpointer + 1) as a "exclusive stop key" in some datasets
    return new com.continuuity.tephra.Transaction(
      Long.MAX_VALUE - 1, wp, new long[0], new long[0],
      com.continuuity.tephra.Transaction.NO_TX_IN_PROGRESS);
  }

  @Override
  public com.continuuity.tephra.Transaction startShort(int timeout) {
    return startShort();
  }

  @Override
  public com.continuuity.tephra.Transaction startLong() {
    return startShort();
  }

  @Override
  public boolean canCommit(com.continuuity.tephra.Transaction tx, Collection<byte[]> changeIds) {
    return true;
  }

  @Override
  public boolean commit(com.continuuity.tephra.Transaction tx) {
    return true;
  }

  @Override
  public void abort(com.continuuity.tephra.Transaction tx) {
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
}
