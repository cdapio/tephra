 /*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.tephra.hbase96;

import co.cask.tephra.TransactionConflictException;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TxConstants;
import co.cask.tephra.hbase96.coprocessor.TransactionProcessor;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import co.cask.tephra.metrics.TxMetricsCollector;
import co.cask.tephra.persist.InMemoryTransactionStateStorage;
import co.cask.tephra.persist.TransactionStateStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for TransactionAwareHTables.
 */
public class TransactionAwareHTableTest {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionAwareHTableTest.class);

  private static HBaseTestingUtility testUtil;
  private static HBaseAdmin hBaseAdmin;
  private static TransactionStateStorage txStateStorage;
  private static TransactionManager txManager;
  private static Configuration conf;
  private TransactionContext transactionContext;
  private TransactionAwareHTable transactionAwareHTable;

  private static final class TestBytes {
    private static final byte[] table = Bytes.toBytes("testtable");
    private static final byte[] family = Bytes.toBytes("f1");
    private static final byte[] family2 = Bytes.toBytes("f2");
    private static final byte[] qualifier = Bytes.toBytes("col1");
    private static final byte[] qualifier2 = Bytes.toBytes("col2");
    private static final byte[] row = Bytes.toBytes("row");
    private static final byte[] value = Bytes.toBytes("value");
    private static final byte[] value2 = Bytes.toBytes("value2");
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
    conf = testUtil.getConfiguration();
    hBaseAdmin = testUtil.getHBaseAdmin();
    txStateStorage = new InMemoryTransactionStateStorage();
    txManager = new TransactionManager
        (conf, txStateStorage, new TxMetricsCollector());
    txManager.startAndWait();
  }

  @AfterClass
  public static void shutdownAfterClass() throws Exception {
    testUtil.shutdownMiniCluster();
    hBaseAdmin.close();
  }

  @Before
  public void setupBeforeTest() throws Exception {
    HTable hTable = createTable(TestBytes.table, new byte[][] {TestBytes.family});
    transactionAwareHTable = new TransactionAwareHTable(hTable);
    transactionContext = new TransactionContext(new InMemoryTxSystemClient(txManager), transactionAwareHTable);
  }

  @After
  public void shutdownAfterTest() throws IOException {
    hBaseAdmin.disableTable(TestBytes.table);
    hBaseAdmin.deleteTable(TestBytes.table);
  }

  private HTable createTable(byte[] tableName, byte[][] columnFamilies) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    for (byte[] family : columnFamilies) {
      HColumnDescriptor columnDesc = new HColumnDescriptor(family);
      columnDesc.setMaxVersions(Integer.MAX_VALUE);
      desc.addFamily(columnDesc);
    }
    desc.addCoprocessor(TransactionProcessor.class.getName());
    hBaseAdmin.createTable(desc);
    testUtil.waitTableAvailable(tableName, 5000);
    return new HTable(testUtil.getConfiguration(), tableName);
  }

  /**
   * Test transactional put and get requests.
   * @throws Exception
   */
  @Test
  public void testValidTransactionalPutAndGet() throws Exception {
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    transactionAwareHTable.put(put);
    transactionContext.finish();

    transactionContext.start();
    Result result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();

    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    assertArrayEquals(TestBytes.value, value);
  }

  /**
   * Test aborted put requests, that must be rolled back.
   * @throws Exception
   */
  @Test
  public void testAbortedTransactionPutAndGet() throws Exception {
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    transactionAwareHTable.put(put);

    transactionContext.abort();

    transactionContext.start();
    Result result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    assertArrayEquals(value, null);
  }

  /**
   * Test transactional delete operations.
   * @throws Exception
   */
  @Test
  public void testValidTransactionalDelete() throws Exception {
    HTable hTable = createTable(Bytes.toBytes("TestValidTransactionalDelete"),
        new byte[][] {TestBytes.family, TestBytes.family2});
    try {
      TransactionAwareHTable txTable = new TransactionAwareHTable(hTable);
      TransactionContext txContext = new TransactionContext(new InMemoryTxSystemClient(txManager), txTable);

      txContext.start();
      Put put = new Put(TestBytes.row);
      put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
      put.add(TestBytes.family2, TestBytes.qualifier, TestBytes.value2);
      txTable.put(put);
      txContext.finish();

      txContext.start();
      Result result = txTable.get(new Get(TestBytes.row));
      txContext.finish();
      byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
      assertArrayEquals(TestBytes.value, value);
      value = result.getValue(TestBytes.family2, TestBytes.qualifier);
      assertArrayEquals(TestBytes.value2, value);

      // test full row delete
      txContext.start();
      Delete delete = new Delete(TestBytes.row);
      txTable.delete(delete);
      txContext.finish();

      txContext.start();
      result = txTable.get(new Get(TestBytes.row));
      txContext.finish();
      assertTrue(result.isEmpty());

      // test column delete
      // load 10 rows
      txContext.start();
      int rowCount = 10;
      for (int i = 0; i < rowCount; i++) {
        Put p = new Put(Bytes.toBytes("row" + i));
        for (int j = 0; j < 10; j++) {
          p.add(TestBytes.family, Bytes.toBytes(j), TestBytes.value);
        }
        txTable.put(p);
      }
      txContext.finish();

      // verify loaded rows
      txContext.start();
      for (int i = 0; i < rowCount; i++) {
        Get g = new Get(Bytes.toBytes("row" + i));
        Result r = txTable.get(g);
        assertFalse(r.isEmpty());
        for (int j = 0; j < 10; j++) {
          assertArrayEquals(TestBytes.value, r.getValue(TestBytes.family, Bytes.toBytes(j)));
        }
      }
      txContext.finish();

      // delete odds columns from odd rows and even columns from even rows
      txContext.start();
      for (int i = 0; i < rowCount; i++) {
        Delete d = new Delete(Bytes.toBytes("row" + i));
        for (int j = 0; j < 10; j++) {
          if (i % 2 == j % 2) {
            LOG.info("Deleting row={}, column={}", i, j);
            d.deleteColumns(TestBytes.family, Bytes.toBytes(j));
          }
        }
        txTable.delete(d);
      }
      txContext.finish();

      // verify deleted columns
      txContext.start();
      for (int i = 0; i < rowCount; i++) {
        Get g = new Get(Bytes.toBytes("row" + i));
        Result r = txTable.get(g);
        assertEquals(5, r.size());
        for (Map.Entry<byte[], byte[]> entry : r.getFamilyMap(TestBytes.family).entrySet()) {
          int col = Bytes.toInt(entry.getKey());
          LOG.info("Got row={}, col={}", i, col);
          // each row should only have the opposite mod (odd=even, even=odd)
          assertNotEquals(i % 2, col % 2);
          assertArrayEquals(TestBytes.value, entry.getValue());
        }
      }
      txContext.finish();

      // test family delete
      // load 10 rows
      txContext.start();
      for (int i = 0; i < rowCount; i++) {
        Put p = new Put(Bytes.toBytes("famrow" + i));
        p.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
        p.add(TestBytes.family2, TestBytes.qualifier2, TestBytes.value2);
        txTable.put(p);
      }
      txContext.finish();

      // verify all loaded rows
      txContext.start();
      for (int i = 0; i < rowCount; i++) {
        Get g = new Get(Bytes.toBytes("famrow" + i));
        Result r = txTable.get(g);
        assertEquals(2, r.size());
        assertArrayEquals(TestBytes.value, r.getValue(TestBytes.family, TestBytes.qualifier));
        assertArrayEquals(TestBytes.value2, r.getValue(TestBytes.family2, TestBytes.qualifier2));
      }
      txContext.finish();

      // delete family1 for even rows, family2 for odd rows
      txContext.start();
      for (int i = 0; i < rowCount; i++) {
        Delete d = new Delete(Bytes.toBytes("famrow" + i));
        d.deleteFamily((i % 2 == 0) ? TestBytes.family : TestBytes.family2);
        txTable.delete(d);
      }
      txContext.finish();

      // verify deleted families
      txContext.start();
      for (int i = 0; i < rowCount; i++) {
        Get g = new Get(Bytes.toBytes("famrow" + i));
        Result r = txTable.get(g);
        assertEquals(1, r.size());
        if (i % 2 == 0) {
          assertNull(r.getValue(TestBytes.family, TestBytes.qualifier));
          assertArrayEquals(TestBytes.value2, r.getValue(TestBytes.family2, TestBytes.qualifier2));
        } else {
          assertArrayEquals(TestBytes.value, r.getValue(TestBytes.family, TestBytes.qualifier));
          assertNull(r.getValue(TestBytes.family2, TestBytes.qualifier2));
        }
      }
      txContext.finish();
    } finally {
      hTable.close();
    }
  }

  /**
   * Test aborted transactional delete requests, that must be rolled back.
   * @throws Exception
   */
  @Test
  public void testAbortedTransactionalDelete() throws Exception {
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    transactionAwareHTable.put(put);
    transactionContext.finish();

    transactionContext.start();
    Result result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    assertArrayEquals(TestBytes.value, value);

    transactionContext.start();
    Delete delete = new Delete(TestBytes.row);
    transactionAwareHTable.delete(delete);
    transactionContext.abort();

    transactionContext.start();
    result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    value = result.getValue(TestBytes.family, TestBytes.qualifier);
    assertArrayEquals(TestBytes.value, value);
  }

  /**
   * Expect an exception since a transaction hasn't been started.
   * @throws Exception
   */
  @Test(expected = IOException.class)
  public void testTransactionlessFailure() throws Exception {
    transactionAwareHTable.get(new Get(TestBytes.row));
  }

  /**
   * Tests that each transaction can see its own persisted writes, while not seeing writes from other
   * in-progress transactions.
   */
  @Test
  public void testReadYourWrites() throws Exception {
    // In-progress tx1: started before our main transaction
    HTable hTable1 = new HTable(testUtil.getConfiguration(), TestBytes.table);
    TransactionAwareHTable txHTable1 = new TransactionAwareHTable(hTable1);
    TransactionContext inprogressTxContext1 = new TransactionContext(new InMemoryTxSystemClient(txManager), txHTable1);

    // In-progress tx2: started while our main transaction is running
    HTable hTable2 = new HTable(testUtil.getConfiguration(), TestBytes.table);
    TransactionAwareHTable txHTable2 = new TransactionAwareHTable(hTable2);
    TransactionContext inprogressTxContext2 = new TransactionContext(new InMemoryTxSystemClient(txManager), txHTable2);

    // create an in-progress write that should be ignored
    byte[] col2 = Bytes.toBytes("col2");
    inprogressTxContext1.start();
    Put putCol2 = new Put(TestBytes.row);
    byte[] valueCol2 = Bytes.toBytes("writing in progress");
    putCol2.add(TestBytes.family, col2, valueCol2);
    txHTable1.put(putCol2);

    // start a tx and write a value to test reading in same tx
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    byte[] value = Bytes.toBytes("writing");
    put.add(TestBytes.family, TestBytes.qualifier, value);
    transactionAwareHTable.put(put);

    // test that a write from a tx started after the first is not visible
    inprogressTxContext2.start();
    Put put2 = new Put(TestBytes.row);
    byte[] value2 = Bytes.toBytes("writing2");
    put2.add(TestBytes.family, TestBytes.qualifier, value2);
    txHTable2.put(put2);

    Get get = new Get(TestBytes.row);
    Result row = transactionAwareHTable.get(get);
    assertFalse(row.isEmpty());
    byte[] col1Value = row.getValue(TestBytes.family, TestBytes.qualifier);
    assertNotNull(col1Value);
    assertArrayEquals(value, col1Value);
    // write from in-progress transaction should not be visible
    byte[] col2Value = row.getValue(TestBytes.family, col2);
    assertNull(col2Value);

    // commit in-progress transaction, should still not be visible
    inprogressTxContext1.finish();

    get = new Get(TestBytes.row);
    row = transactionAwareHTable.get(get);
    assertFalse(row.isEmpty());
    col2Value = row.getValue(TestBytes.family, col2);
    assertNull(col2Value);

    transactionContext.finish();

    inprogressTxContext2.abort();
  }

  @Test
  public void testRowLevelConflictDetection() throws Exception {
    TransactionAwareHTable txTable1 = new TransactionAwareHTable(new HTable(conf, TestBytes.table),
        TxConstants.ConflictDetection.ROW);
    TransactionContext txContext1 = new TransactionContext(new InMemoryTxSystemClient(txManager), txTable1);

    TransactionAwareHTable txTable2 = new TransactionAwareHTable(new HTable(conf, TestBytes.table),
        TxConstants.ConflictDetection.ROW);
    TransactionContext txContext2 = new TransactionContext(new InMemoryTxSystemClient(txManager), txTable2);

    byte[] row1 = Bytes.toBytes("row1");
    byte[] row2 = Bytes.toBytes("row2");
    byte[] col1 = Bytes.toBytes("c1");
    byte[] col2 = Bytes.toBytes("c2");
    byte[] val1 = Bytes.toBytes("val1");
    byte[] val2 = Bytes.toBytes("val2");

    // test that concurrent writing to different rows succeeds
    txContext1.start();
    txTable1.put(new Put(row1).add(TestBytes.family, col1, val1));

    txContext2.start();
    txTable2.put(new Put(row2).add(TestBytes.family, col1, val2));

    // should be no conflicts
    txContext1.finish();
    txContext2.finish();

    transactionContext.start();
    Result res = transactionAwareHTable.get(new Get(row1));
    assertFalse(res.isEmpty());
    Cell cell = res.getColumnLatestCell(TestBytes.family, col1);
    assertNotNull(cell);
    assertArrayEquals(val1, CellUtil.cloneValue(cell));

    res = transactionAwareHTable.get(new Get(row2));
    assertFalse(res.isEmpty());
    cell = res.getColumnLatestCell(TestBytes.family, col1);
    assertNotNull(cell);
    assertArrayEquals(val2, CellUtil.cloneValue(cell));
    transactionContext.finish();

    // test that writing to different columns in the same row fails
    txContext1.start();
    txTable1.put(new Put(row1).add(TestBytes.family, col1, val2));

    txContext2.start();
    txTable2.put(new Put(row1).add(TestBytes.family, col2, val2));

    txContext1.finish();
    try {
      txContext2.finish();
      fail("txContext2 should have encountered a row-level conflict during commit");
    } catch (TransactionConflictException tce) {
      txContext2.abort();
    }

    transactionContext.start();
    res = transactionAwareHTable.get(new Get(row1));
    assertFalse(res.isEmpty());
    cell = res.getColumnLatestCell(TestBytes.family, col1);
    assertNotNull(cell);
    // should now be val2
    assertArrayEquals(val2, CellUtil.cloneValue(cell));

    cell = res.getColumnLatestCell(TestBytes.family, col2);
    // col2 should not be visible due to conflict
    assertNull(cell);
    transactionContext.finish();

    // test that writing to the same column in the same row fails
    txContext1.start();
    txTable1.put(new Put(row2).add(TestBytes.family, col2, val1));

    txContext2.start();
    txTable2.put(new Put(row2).add(TestBytes.family, col2, val2));

    txContext1.finish();
    try {
      txContext2.finish();
      fail("txContext2 should have encountered a row and column level conflict during commit");
    } catch (TransactionConflictException tce) {
      txContext2.abort();
    }

    transactionContext.start();
    res = transactionAwareHTable.get(new Get(row2));
    assertFalse(res.isEmpty());
    cell = res.getColumnLatestCell(TestBytes.family, col2);
    assertNotNull(cell);
    // should now be val1
    assertArrayEquals(val1, CellUtil.cloneValue(cell));
    transactionContext.finish();

    // verify change set that is being reported only on rows
    txContext1.start();
    txTable1.put(new Put(row1).add(TestBytes.family, col1, val1));
    txTable1.put(new Put(row2).add(TestBytes.family, col2, val2));

    Collection<byte[]> changeSet = txTable1.getTxChanges();
    assertNotNull(changeSet);
    assertEquals(2, changeSet.size());
    assertTrue(changeSet.contains(txTable1.getChangeKey(row1, null, null)));
    assertTrue(changeSet.contains(txTable1.getChangeKey(row2, null, null)));
    txContext1.finish();
  }
}
