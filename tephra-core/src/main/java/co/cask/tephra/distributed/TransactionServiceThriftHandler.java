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

package co.cask.tephra.distributed;

import co.cask.tephra.InvalidTruncateTimeException;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionNotInProgressException;
import co.cask.tephra.TxConstants;
import co.cask.tephra.distributed.thrift.TBoolean;
import co.cask.tephra.distributed.thrift.TInvalidTruncateTimeException;
import co.cask.tephra.distributed.thrift.TTransaction;
import co.cask.tephra.distributed.thrift.TTransactionCouldNotTakeSnapshotException;
import co.cask.tephra.distributed.thrift.TTransactionNotInProgressException;
import co.cask.tephra.distributed.thrift.TTransactionServer;
import co.cask.tephra.rpc.RPCServiceHandler;
import com.google.common.collect.Sets;
import org.apache.thrift.TException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * The implementation of a thrift service for tx service.
 * All operations arrive over the wire as Thrift objects.
 * <p/>
 * Why all this conversion (wrap/unwrap), and not define all Operations
 * themselves as Thrift objects?
 * <ul><li>
 * All the non-thrift executors would have to use the Thrift objects
 * </li><li>
 * Thrift's object model is too restrictive: it has only limited inheritance
 * and no overloading
 * </li><li>
 * Thrift objects are bare-bone, all they have are getters, setters, and
 * basic object methods.
 * </li></ul>
 */
public class TransactionServiceThriftHandler implements TTransactionServer.Iface, RPCServiceHandler {

  private final TransactionManager txManager;

  public TransactionServiceThriftHandler(TransactionManager txManager) {
    this.txManager = txManager;
  }

  @Override
  public TTransaction startLong() throws TException {
    return TransactionConverterUtils.wrap(txManager.startLong());
  }

  @Override
  public TTransaction startShort() throws TException {
    return TransactionConverterUtils.wrap(txManager.startShort());
  }

  @Override
  public TTransaction startShortTimeout(int timeout) throws TException {
    return TransactionConverterUtils.wrap(txManager.startShort(timeout));
  }


  @Override
  public TBoolean canCommitTx(TTransaction tx, Set<ByteBuffer> changes) throws TException {

    Set<byte[]> changeIds = Sets.newHashSet();
    for (ByteBuffer bb : changes) {
      byte[] changeId = new byte[bb.remaining()];
      bb.get(changeId);
      changeIds.add(changeId);
    }
    try {
      return new TBoolean(txManager.canCommit(TransactionConverterUtils.unwrap(tx), changeIds));
    } catch (TransactionNotInProgressException e) {
      throw new TTransactionNotInProgressException(e.getMessage());
    }
  }

  @Override
  public TBoolean commitTx(TTransaction tx) throws TException {
    try {
      return new TBoolean(txManager.commit(TransactionConverterUtils.unwrap(tx)));
    } catch (TransactionNotInProgressException e) {
      throw new TTransactionNotInProgressException(e.getMessage());
    }
  }

  @Override
  public void abortTx(TTransaction tx) throws TException {
    txManager.abort(TransactionConverterUtils.unwrap(tx));
  }

  @Override
  public boolean invalidateTx(long tx) throws TException {
    return txManager.invalidate(tx);
  }

  @Override
  public void init() throws Exception {
    txManager.startAndWait();
  }

  @Override
  public void destroy() throws Exception {
    txManager.stopAndWait();
  }

  @Override
  public ByteBuffer getSnapshot() throws TException {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
        boolean snapshotTaken = txManager.takeSnapshot(out);
        if (!snapshotTaken) {
          throw new TTransactionCouldNotTakeSnapshotException("Transaction manager could not get a snapshot.");
        }
      } finally {
        out.close();
      }
      // todo find a way to encode directly to the stream, without having the snapshot in memory twice
      return ByteBuffer.wrap(out.toByteArray());
    } catch (IOException e) {
      throw new TTransactionCouldNotTakeSnapshotException(e.getMessage());
    }
  }

  @Override
  public void resetState() throws TException {
    txManager.resetState();
  }

  @Override
  public String status() throws TException {
    return txManager.isRunning() ? TxConstants.STATUS_OK : TxConstants.STATUS_NOTOK;
  }

  @Override
  public TBoolean truncateInvalidTx(Set<Long> txns) throws TException {
    return new TBoolean(txManager.truncateInvalidTx(txns));
  }

  @Override
  public TBoolean truncateInvalidTxBefore(long time) throws TException {
    try {
      return new TBoolean(txManager.truncateInvalidTxBefore(time));
    } catch (InvalidTruncateTimeException e) {
      throw new TInvalidTruncateTimeException(e.getMessage());
    }
  }

  @Override
  public int invalidTxSize() throws TException {
    return txManager.getInvalidSize();
  }
}
