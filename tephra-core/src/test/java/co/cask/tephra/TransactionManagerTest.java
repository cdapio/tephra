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

package co.cask.tephra;

import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import co.cask.tephra.metrics.TxMetricsCollector;
import co.cask.tephra.persist.InMemoryTransactionStateStorage;
import co.cask.tephra.persist.TransactionStateStorage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TransactionManagerTest extends TransactionSystemTest {

  static Configuration conf = new Configuration();

  TransactionManager txManager = null;
  TransactionStateStorage txStateStorage = null;

  @Override
  protected TransactionSystemClient getClient() {
    return new InMemoryTxSystemClient(txManager);
  }

  @Override
  protected TransactionStateStorage getStateStorage() {
    return txStateStorage;
  }

  @Before
  public void before() {
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 0); // no cleanup thread
    // todo should create two sets of tests, one with LocalFileTxStateStorage and one with InMemoryTxStateStorage
    txStateStorage = new InMemoryTransactionStateStorage();
    txManager = new TransactionManager
      (conf, txStateStorage, new TxMetricsCollector());
    txManager.startAndWait();
  }

  @After
  public void after() {
    txManager.stopAndWait();
  }

  @Test
  public void testTransactionCleanup() throws Exception {
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 3);
    conf.setInt(TxConstants.Manager.CFG_TX_TIMEOUT, 2);
    // using a new tx manager that cleans up
    TransactionManager txm = new TransactionManager
      (conf, new InMemoryTransactionStateStorage(), new TxMetricsCollector());
    txm.startAndWait();
    try {
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());
      // start a transaction and leave it open
      Transaction tx1 = txm.startShort();
      // start a long running transaction and leave it open
      Transaction tx2 = txm.startLong();
      Transaction tx3 = txm.startLong();
      // start and commit a bunch of transactions
      for (int i = 0; i < 10; i++) {
        Transaction tx = txm.startShort();
        Assert.assertTrue(txm.canCommit(tx, Collections.singleton(new byte[] { (byte) i })));
        Assert.assertTrue(txm.commit(tx));
      }
      // all of these should still be in the committed set
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(10, txm.getCommittedSize());
      // sleep longer than the cleanup interval
      TimeUnit.SECONDS.sleep(5);
      // transaction should now be invalid
      Assert.assertEquals(1, txm.getInvalidSize());
      // run another transaction
      Transaction txx = txm.startShort();
      // verify the exclude
      Assert.assertFalse(txx.isVisible(tx1.getWritePointer()));
      Assert.assertFalse(txx.isVisible(tx2.getWritePointer()));
      Assert.assertFalse(txx.isVisible(tx3.getWritePointer()));
      // try to commit the last transaction that was started
      Assert.assertTrue(txm.canCommit(txx, Collections.singleton(new byte[] { 0x0a })));
      Assert.assertTrue(txm.commit(txx));

      // now the committed change sets should be empty again
      Assert.assertEquals(0, txm.getCommittedSize());
      // cannot commit transaction as it was timed out
      try {
        txm.canCommit(tx1, Collections.singleton(new byte[] { 0x11 }));
        Assert.fail();
      } catch (TransactionNotInProgressException e) {
        // expected
      }
      txm.abort(tx1);
      // abort should have removed from invalid
      Assert.assertEquals(0, txm.getInvalidSize());
      // run another bunch of transactions
      for (int i = 0; i < 10; i++) {
        Transaction tx = txm.startShort();
        Assert.assertTrue(txm.canCommit(tx, Collections.singleton(new byte[] { (byte) i })));
        Assert.assertTrue(txm.commit(tx));
      }
      // none of these should still be in the committed set (tx2 is long-running).
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());
      // commit tx2, abort tx3
      Assert.assertTrue(txm.commit(tx2));
      txm.abort(tx3);
      // none of these should still be in the committed set (tx2 is long-running).
      // Only tx3 is invalid list as it was aborted and is long-running. tx1 is short one and it rolled back its changes
      // so it should NOT be in invalid list
      Assert.assertEquals(1, txm.getInvalidSize());
      Assert.assertEquals(tx3.getWritePointer(), (long) txm.getCurrentState().getInvalid().iterator().next());
      Assert.assertEquals(1, txm.getExcludedListSize());
    } finally {
      txm.stopAndWait();
    }
  }

  @Test
  public void testLongTransactionCleanup() throws Exception {
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 3);
    conf.setInt(TxConstants.Manager.CFG_TX_LONG_TIMEOUT, 2);
    // using a new tx manager that cleans up
    TransactionManager txm = new TransactionManager
      (conf, new InMemoryTransactionStateStorage(), new TxMetricsCollector());
    txm.startAndWait();
    try {
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());
      
      // start a long running transaction
      Transaction tx1 = txm.startLong();
      
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());

      // sleep longer than the cleanup interval
      TimeUnit.SECONDS.sleep(5);

      // transaction should now be invalid
      Assert.assertEquals(1, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());

      // cannot commit transaction as it was timed out
      try {
        txm.canCommit(tx1, Collections.singleton(new byte[] { 0x11 }));
        Assert.fail();
      } catch (TransactionNotInProgressException e) {
        // expected
      }
      
      txm.abort(tx1);
      // abort should not remove long running transaction from invalid list
      Assert.assertEquals(1, txm.getInvalidSize());
    } finally {
      txm.stopAndWait();
    }
  }
  
  @Test
  public void testTruncateInvalid() throws Exception {
    InMemoryTransactionStateStorage storage = new InMemoryTransactionStateStorage();
    Configuration testConf = new Configuration(conf);
    // No snapshots
    testConf.setLong(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL, -1);
    TransactionManager txm1 = new TransactionManager(testConf, storage, new TxMetricsCollector());
    txm1.startAndWait();

    TransactionManager txm2 = null;
    Transaction tx1;
    Transaction tx2;
    Transaction tx3;
    Transaction tx4;
    Transaction tx5;
    Transaction tx6;
    try {
      Assert.assertEquals(0, txm1.getInvalidSize());

      // start a few transactions
      tx1 = txm1.startLong();
      tx2 = txm1.startShort();
      tx3 = txm1.startLong();
      tx4 = txm1.startShort();
      tx5 = txm1.startLong();
      tx6 = txm1.startShort();

      // invalidate tx1, tx2, tx5 and tx6
      txm1.invalidate(tx1.getWritePointer());
      txm1.invalidate(tx2.getWritePointer());
      txm1.invalidate(tx5.getWritePointer());
      txm1.invalidate(tx6.getWritePointer());

      // tx1, tx2, tx5 and tx6 should be in invalid list
      Assert.assertEquals(
        ImmutableList.of(tx1.getWritePointer(), tx2.getWritePointer(), tx5.getWritePointer(), tx6.getWritePointer()),
        txm1.getCurrentState().getInvalid()
      );
      
      // remove tx1 and tx6 from invalid list
      Assert.assertTrue(txm1.truncateInvalidTx(ImmutableSet.of(tx1.getWritePointer(), tx6.getWritePointer())));
      
      // only tx2 and tx5 should be in invalid list now
      Assert.assertEquals(ImmutableList.of(tx2.getWritePointer(), tx5.getWritePointer()),
                          txm1.getCurrentState().getInvalid());
      
      // removing in-progress transactions should not have any effect
      Assert.assertEquals(ImmutableSet.of(tx3.getWritePointer(), tx4.getWritePointer()),
                          txm1.getCurrentState().getInProgress().keySet());
      Assert.assertFalse(txm1.truncateInvalidTx(ImmutableSet.of(tx3.getWritePointer(), tx4.getWritePointer())));
      // no change to in-progress
      Assert.assertEquals(ImmutableSet.of(tx3.getWritePointer(), tx4.getWritePointer()),
                          txm1.getCurrentState().getInProgress().keySet());
      // no change to invalid list
      Assert.assertEquals(ImmutableList.of(tx2.getWritePointer(), tx5.getWritePointer()),
                          txm1.getCurrentState().getInvalid());

      // Test transaction edit logs replay
      // Start another transaction manager without stopping txm1 so that snapshot does not get written,
      // and all logs can be replayed.
      txm2 = new TransactionManager(testConf, storage, new TxMetricsCollector());
      txm2.startAndWait();
      Assert.assertEquals(ImmutableList.of(tx2.getWritePointer(), tx5.getWritePointer()),
                          txm2.getCurrentState().getInvalid());
      Assert.assertEquals(ImmutableSet.of(tx3.getWritePointer(), tx4.getWritePointer()),
                          txm2.getCurrentState().getInProgress().keySet());
    } finally {
      txm1.stopAndWait();
      if (txm2 != null) {
        txm2.stopAndWait();
      }
    }
  }

  @Test
  public void testTruncateInvalidBeforeTime() throws Exception {
    InMemoryTransactionStateStorage storage = new InMemoryTransactionStateStorage();
    Configuration testConf = new Configuration(conf);
    // No snapshots
    testConf.setLong(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL, -1);
    TransactionManager txm1 = new TransactionManager(testConf, storage, new TxMetricsCollector());
    txm1.startAndWait();

    TransactionManager txm2 = null;
    Transaction tx1;
    Transaction tx2;
    Transaction tx3;
    Transaction tx4;
    Transaction tx5;
    Transaction tx6;
    try {
      Assert.assertEquals(0, txm1.getInvalidSize());

      // start a few transactions
      tx1 = txm1.startLong();
      tx2 = txm1.startShort();
      // Sleep so that transaction ids get generated a millisecond apart for assertion
      // TEPHRA-63 should eliminate the need to sleep
      TimeUnit.MILLISECONDS.sleep(1);
      long timeBeforeTx3 = System.currentTimeMillis();
      tx3 = txm1.startLong();
      tx4 = txm1.startShort();
      TimeUnit.MILLISECONDS.sleep(1);
      long timeBeforeTx5 = System.currentTimeMillis();
      tx5 = txm1.startLong();
      tx6 = txm1.startShort();

      // invalidate tx1, tx2, tx5 and tx6
      txm1.invalidate(tx1.getWritePointer());
      txm1.invalidate(tx2.getWritePointer());
      txm1.invalidate(tx5.getWritePointer());
      txm1.invalidate(tx6.getWritePointer());

      // tx1, tx2, tx5 and tx6 should be in invalid list
      Assert.assertEquals(
        ImmutableList.of(tx1.getWritePointer(), tx2.getWritePointer(), tx5.getWritePointer(), tx6.getWritePointer()),
        txm1.getCurrentState().getInvalid()
      );

      // remove transactions before tx3 from invalid list
      Assert.assertTrue(txm1.truncateInvalidTxBefore(timeBeforeTx3));

      // only tx5 and tx6 should be in invalid list now
      Assert.assertEquals(ImmutableList.of(tx5.getWritePointer(), tx6.getWritePointer()),
                          txm1.getCurrentState().getInvalid());

      // removing invalid transactions before tx5 should throw exception since tx3 and tx4 are in-progress
      Assert.assertEquals(ImmutableSet.of(tx3.getWritePointer(), tx4.getWritePointer()),
                          txm1.getCurrentState().getInProgress().keySet());
      try {
        txm1.truncateInvalidTxBefore(timeBeforeTx5);
        Assert.fail("Expected InvalidTruncateTimeException exception");
      } catch (InvalidTruncateTimeException e) {
        // Expected exception
      }
      // no change to in-progress
      Assert.assertEquals(ImmutableSet.of(tx3.getWritePointer(), tx4.getWritePointer()),
                          txm1.getCurrentState().getInProgress().keySet());
      // no change to invalid list
      Assert.assertEquals(ImmutableList.of(tx5.getWritePointer(), tx6.getWritePointer()),
                          txm1.getCurrentState().getInvalid());

      // Test transaction edit logs replay
      // Start another transaction manager without stopping txm1 so that snapshot does not get written, 
      // and all logs can be replayed.
      txm2 = new TransactionManager(testConf, storage, new TxMetricsCollector());
      txm2.startAndWait();
      Assert.assertEquals(ImmutableList.of(tx5.getWritePointer(), tx6.getWritePointer()),
                          txm2.getCurrentState().getInvalid());
      Assert.assertEquals(ImmutableSet.of(tx3.getWritePointer(), tx4.getWritePointer()),
                          txm2.getCurrentState().getInProgress().keySet());
    } finally {
      txm1.stopAndWait();
      if (txm2 != null) {
        txm2.stopAndWait();
      }
    }
  }
}
