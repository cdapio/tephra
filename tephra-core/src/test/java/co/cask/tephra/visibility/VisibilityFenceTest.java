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

package co.cask.tephra.visibility;

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionConflictException;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import co.cask.tephra.metrics.TxMetricsCollector;
import co.cask.tephra.persist.InMemoryTransactionStateStorage;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The following are all the possible cases when using {@link VisibilityFence}.
 *
 * In the below table,
 * "Read Txn" refers to the transaction that contains the read fence
 * "Before Write", "During Write" and "After Write" refer to the write transaction time
 * "Before Write Fence", "During Write Fence", "After Write Fence" refer to the write fence transaction time
 *
 * Timeline is: Before Write < During Write < After Write < Before Write Fence < During Write Fence <
 *              After Write Fence
 *
 * +------+----------------------+----------------------+--------------------+--------------------+
 * | Case |    Read Txn Start    |   Read Txn Commit    | Conflict on Commit | Conflict on Commit |
 * |      |                      |                      | of Read Txn        | of Write Fence     |
 * +------+----------------------+----------------------+--------------------+--------------------+
 * |    1 | Before Write         | Before Write         | No                 | No                 |
 * |    2 | Before Write         | During Write         | No                 | No                 |
 * |    3 | Before Write         | After Write          | No                 | No                 |
 * |    4 | Before Write         | Before Write Fence   | No                 | No                 |
 * |    5 | Before Write         | During Write Fence   | No                 | Yes                |
 * |    6 | Before Write         | After Write Fence    | Yes                | No                 |
 * |      |                      |                      |                    |                    |
 * |    7 | During Write         | During Write         | No                 | No                 |
 * |    8 | During Write         | After Write          | No                 | No                 |
 * |    9 | During Write         | Before Write Fence   | No                 | No                 |
 * |   10 | During Write         | During Write Fence   | No                 | Yes                |
 * |   11 | During Write         | After Write Fence    | Yes                | No                 |
 * |      |                      |                      |                    |                    |
 * |   12 | After Write          | After Write          | No                 | No                 |
 * |   13 | After Write          | Before Write Fence   | No                 | No                 |
 * |   14 | After Write          | During Write Fence   | No                 | Yes #              |
 * |   15 | After Write          | After Write Fence    | Yes #              | No                 |
 * |      |                      |                      |                    |                    |
 * |   16 | Before Write Fence   | Before Write Fence   | No                 | No                 |
 * |   17 | Before Write Fence   | During Write Fence   | No                 | Yes #              |
 * |   18 | Before Write Fence   | After Write Fence    | Yes #              | No                 |
 * |      |                      |                      |                    |                    |
 * |   19 | During Write Fence   | During Write Fence   | No                 | No                 |
 * |   20 | During Write Fence   | After Write Fence    | No                 | No                 |
 * |      |                      |                      |                    |                    |
 * |   21 | After Write Fence    | After Write Fence    | No                 | No                 |
 * +------+----------------------+----------------------+--------------------+--------------------+
 *
 * Note: Cases marked with '#' in conflict column should not conflict, however current implementation causes
 * them to conflict. The remaining conflicts are a result of the fence.
 *
 * In the current implementation of VisibilityFence, read txns that start "Before Write", "During Write",
 * and "After Write" can be represented by read txns that start "Before Write Fence".
 * Verifying cases 16, 17, 18, 20 and 21 will effectively cover all other cases.
 */
public class VisibilityFenceTest {
  private static Configuration conf = new Configuration();

  private static TransactionManager txManager = null;

  @BeforeClass
  public static void before() {
    txManager = new TransactionManager(conf, new InMemoryTransactionStateStorage(), new TxMetricsCollector());
    txManager.startAndWait();
  }

  @AfterClass
  public static void after() {
    txManager.stopAndWait();
  }

  @Test
  public void testFence1() throws Exception {
    byte[] fenceId = "test_table".getBytes(Charsets.UTF_8);

    // Writer updates data here in a separate transaction (code not shown)
    // start tx
    // update
    // commit tx

    // Readers use fence to indicate that they are interested in changes to specific data
    TransactionAware readFenceCase16 = VisibilityFence.create(fenceId);
    TransactionContext readTxContextCase16 =
      new TransactionContext(new InMemoryTxSystemClient(txManager), readFenceCase16);
    readTxContextCase16.start();
    readTxContextCase16.finish();

    TransactionAware readFenceCase17 = VisibilityFence.create(fenceId);
    TransactionContext readTxContextCase17 =
      new TransactionContext(new InMemoryTxSystemClient(txManager), readFenceCase17);
    readTxContextCase17.start();

    TransactionAware readFenceCase18 = VisibilityFence.create(fenceId);
    TransactionContext readTxContextCase18 =
      new TransactionContext(new InMemoryTxSystemClient(txManager), readFenceCase18);
    readTxContextCase18.start();

    // Now writer needs to wait for in-progress readers to see the change, it uses write fence to do so
    // Start write fence txn
    TransactionAware writeFence = new WriteFence(fenceId);
    TransactionContext writeTxContext = new TransactionContext(new InMemoryTxSystemClient(txManager), writeFence);
    writeTxContext.start();

    TransactionAware readFenceCase20 = VisibilityFence.create(fenceId);
    TransactionContext readTxContextCase20 =
      new TransactionContext(new InMemoryTxSystemClient(txManager), readFenceCase20);
    readTxContextCase20.start();

    readTxContextCase17.finish();

    assertTxnConflict(writeTxContext);
    writeTxContext.start();

    // Commit write fence txn can commit without conflicts at this point
    writeTxContext.finish();

    TransactionAware readFenceCase21 = VisibilityFence.create(fenceId);
    TransactionContext readTxContextCase21 =
      new TransactionContext(new InMemoryTxSystemClient(txManager), readFenceCase21);
    readTxContextCase21.start();

    assertTxnConflict(readTxContextCase18);
    readTxContextCase20.finish();
    readTxContextCase21.finish();
  }

  private void assertTxnConflict(TransactionContext txContext) throws Exception {
    try {
      txContext.finish();
      Assert.fail("Expected transaction to fail");
    } catch (TransactionConflictException e) {
      // Expected
      txContext.abort();
    }
  }

  @Test
  public void testFence2() throws Exception {
    byte[] fenceId = "test_table".getBytes(Charsets.UTF_8);

    // Readers use fence to indicate that they are interested in changes to specific data
    // Reader 1
    TransactionAware readFence1 = VisibilityFence.create(fenceId);
    TransactionContext readTxContext1 = new TransactionContext(new InMemoryTxSystemClient(txManager), readFence1);
    readTxContext1.start();

    // Reader 2
    TransactionAware readFence2 = VisibilityFence.create(fenceId);
    TransactionContext readTxContext2 = new TransactionContext(new InMemoryTxSystemClient(txManager), readFence2);
    readTxContext2.start();

    // Reader 3
    TransactionAware readFence3 = VisibilityFence.create(fenceId);
    TransactionContext readTxContext3 = new TransactionContext(new InMemoryTxSystemClient(txManager), readFence3);
    readTxContext3.start();

    // Writer updates data here in a separate transaction (code not shown)
    // start tx
    // update
    // commit tx

    // Now writer needs to wait for readers 1, 2, and 3 to see the change, it uses write fence to do so
    TransactionAware writeFence = new WriteFence(fenceId);
    TransactionContext writeTxContext = new TransactionContext(new InMemoryTxSystemClient(txManager), writeFence);
    writeTxContext.start();

    // Reader 1 commits before writeFence is committed
    readTxContext1.finish();

    try {
      // writeFence will throw exception since Reader 1 committed without seeing changes
      writeTxContext.finish();
      Assert.fail("Expected transaction to fail");
    } catch (TransactionConflictException e) {
      // Expected
      writeTxContext.abort();
    }

    // Start over writeFence again
    writeTxContext.start();

    // Now, Reader 3 commits before writeFence
    // Note that Reader 3 does not conflict with Reader 1
    readTxContext3.finish();

    try {
      // writeFence will throw exception again since Reader 3 committed without seeing changes
      writeTxContext.finish();
      Assert.fail("Expected transaction to fail");
    } catch (TransactionConflictException e) {
      // Expected
      writeTxContext.abort();
    }

    // Start over writeFence again
    writeTxContext.start();
    // This time writeFence commits before the other readers
    writeTxContext.finish();

    // After this point all readers will see the change

    try {
      // Reader 2 commits after writeFence, hence this commit with throw exception
      readTxContext2.finish();
      Assert.fail("Expected transaction to fail");
    } catch (TransactionConflictException e) {
      // Expected
      readTxContext2.abort();
    }

    // Reader 2 has to abort and start over again. It will see the changes now.
    readTxContext2 = new TransactionContext(new InMemoryTxSystemClient(txManager), readFence2);
    readTxContext2.start();
    readTxContext2.finish();
  }

  @Test
  public void testFenceAwait() throws Exception {
    byte[] fenceId = "test_table".getBytes(Charsets.UTF_8);

    final TransactionContext fence1 = new TransactionContext(new InMemoryTxSystemClient(txManager),
                                                       VisibilityFence.create(fenceId));
    fence1.start();
    final TransactionContext fence2 = new TransactionContext(new InMemoryTxSystemClient(txManager),
                                                       VisibilityFence.create(fenceId));
    fence2.start();
    TransactionContext fence3 = new TransactionContext(new InMemoryTxSystemClient(txManager),
                                                       VisibilityFence.create(fenceId));
    fence3.start();

    final AtomicInteger attempts = new AtomicInteger();
    TransactionSystemClient customTxClient = new InMemoryTxSystemClient(txManager) {
      @Override
      public Transaction startShort() {
        Transaction transaction = super.startShort();
        try {
          switch (attempts.getAndIncrement()) {
            case 0:
              fence1.finish();
              break;
            case 1:
              fence2.finish();
              break;
            case 2:
              break;
            default:
              throw new IllegalStateException("Unexpected state");
          }
        } catch (TransactionFailureException e) {
          Throwables.propagate(e);
        }
        return transaction;
      }
    };

    FenceWait fenceWait = VisibilityFence.prepareWait(fenceId, customTxClient);
    fenceWait.await(1000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(3, attempts.get());

    try {
      fence3.finish();
      Assert.fail("Expected transaction to fail");
    } catch (TransactionConflictException e) {
      // Expected exception
      fence3.abort();
    }

    fence3.start();
    fence3.finish();
  }

  @Test
  public void testFenceTimeout() throws Exception {
    byte[] fenceId = "test_table".getBytes(Charsets.UTF_8);

    final TransactionContext fence1 = new TransactionContext(new InMemoryTxSystemClient(txManager),
                                                             VisibilityFence.create(fenceId));
    fence1.start();

    final long timeout = 100;
    final TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    final AtomicInteger attempts = new AtomicInteger();
    TransactionSystemClient customTxClient = new InMemoryTxSystemClient(txManager) {
      @Override
      public Transaction startShort() {
        Transaction transaction = super.startShort();
        try {
          switch (attempts.getAndIncrement()) {
            case 0:
              fence1.finish();
              break;
          }
          timeUnit.sleep(timeout + 1);
        } catch (InterruptedException | TransactionFailureException e) {
          Throwables.propagate(e);
        }
        return transaction;
      }
    };

    try {
      FenceWait fenceWait = VisibilityFence.prepareWait(fenceId, customTxClient);
      fenceWait.await(timeout, timeUnit);
      Assert.fail("Expected await to fail");
    } catch (TimeoutException e) {
      // Expected exception
    }
    Assert.assertEquals(1, attempts.get());

    FenceWait fenceWait = VisibilityFence.prepareWait(fenceId, customTxClient);
    fenceWait.await(timeout, timeUnit);
    Assert.assertEquals(2, attempts.get());
  }
}
