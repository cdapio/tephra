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
package co.cask.tephra.hbase10cdh;

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionConflictException;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
import co.cask.tephra.hbase10cdh.coprocessor.TransactionProcessor;
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
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
    private static final byte[] row2 = Bytes.toBytes("row2");
    private static final byte[] row3 = Bytes.toBytes("row3");
    private static final byte[] row4 = Bytes.toBytes("row4");
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
    txManager = new TransactionManager(conf, txStateStorage, new TxMetricsCollector());
    txManager.startAndWait();
  }

  @AfterClass
  public static void shutdownAfterClass() throws Exception {
    testUtil.shutdownMiniCluster();
    hBaseAdmin.close();
  }

  @Before
  public void setupBeforeTest() throws Exception {
    HTable hTable = createTable(TestBytes.table, new byte[][]{TestBytes.family});
    transactionAwareHTable = new TransactionAwareHTable(hTable);
    transactionContext = new TransactionContext(new InMemoryTxSystemClient(txManager), transactionAwareHTable);
  }

  @After
  public void shutdownAfterTest() throws IOException {
    hBaseAdmin.disableTable(TestBytes.table);
    hBaseAdmin.deleteTable(TestBytes.table);
  }

  private HTable createTable(byte[] tableName, byte[][] columnFamilies) throws Exception {
    return createTable(tableName, columnFamilies, false);
  }

  private HTable createTable(byte[] tableName, byte[][] columnFamilies, boolean existingData) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    for (byte[] family : columnFamilies) {
      HColumnDescriptor columnDesc = new HColumnDescriptor(family);
      columnDesc.setMaxVersions(Integer.MAX_VALUE);
      columnDesc.setValue(TxConstants.PROPERTY_TTL, String.valueOf(100000)); // in millis
      desc.addFamily(columnDesc);
    }
    if (existingData) {
      desc.setValue(TxConstants.READ_NON_TX_DATA, "true");
    }
    desc.addCoprocessor(TransactionProcessor.class.getName());
    hBaseAdmin.createTable(desc);
    testUtil.waitTableAvailable(tableName, 5000);
    return new HTable(testUtil.getConfiguration(), tableName);
  }

  /**
   * Test transactional put and get requests.
   *
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
   *
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
   *
   * @throws Exception
   */
  @Test
  public void testValidTransactionalDelete() throws Exception {
    HTable hTable = createTable(Bytes.toBytes("TestValidTransactionalDelete"),
        new byte[][]{TestBytes.family, TestBytes.family2});
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
   *
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

  @Test
  public void testRowDelete() throws Exception {
    HTable hTable = createTable(Bytes.toBytes("TestRowDelete"), new byte[][]{TestBytes.family, TestBytes.family2});
    TransactionAwareHTable txTable = new TransactionAwareHTable(hTable, TxConstants.ConflictDetection.ROW);
    try {
      TransactionContext txContext = new TransactionContext(new InMemoryTxSystemClient(txManager), txTable);

      // Test 1: full row delete
      txContext.start();
      txTable.put(new Put(TestBytes.row)
          .add(TestBytes.family, TestBytes.qualifier, TestBytes.value)
          .add(TestBytes.family, TestBytes.qualifier2, TestBytes.value2)
          .add(TestBytes.family2, TestBytes.qualifier, TestBytes.value)
          .add(TestBytes.family2, TestBytes.qualifier2, TestBytes.value2));
      txContext.finish();

      txContext.start();
      Get get = new Get(TestBytes.row);
      Result result = txTable.get(get);
      assertFalse(result.isEmpty());
      assertArrayEquals(TestBytes.value, result.getValue(TestBytes.family, TestBytes.qualifier));
      assertArrayEquals(TestBytes.value2, result.getValue(TestBytes.family, TestBytes.qualifier2));
      assertArrayEquals(TestBytes.value, result.getValue(TestBytes.family2, TestBytes.qualifier));
      assertArrayEquals(TestBytes.value2, result.getValue(TestBytes.family2, TestBytes.qualifier2));
      txContext.finish();

      // delete entire row
      txContext.start();
      txTable.delete(new Delete(TestBytes.row));
      txContext.finish();

      // verify row is now empty
      txContext.start();
      result = txTable.get(new Get(TestBytes.row));
      assertTrue(result.isEmpty());

      // verify row is empty for explicit column retrieval
      result = txTable.get(new Get(TestBytes.row)
          .addColumn(TestBytes.family, TestBytes.qualifier)
          .addFamily(TestBytes.family2));
      assertTrue(result.isEmpty());

      // verify row is empty for scan
      ResultScanner scanner = txTable.getScanner(new Scan(TestBytes.row));
      assertNull(scanner.next());
      scanner.close();

      // verify row is empty for scan with explicit column
      scanner = txTable.getScanner(new Scan(TestBytes.row).addColumn(TestBytes.family2, TestBytes.qualifier2));
      assertNull(scanner.next());
      scanner.close();
      txContext.finish();

      // write swapped values to one column per family
      txContext.start();
      txTable.put(new Put(TestBytes.row)
          .add(TestBytes.family, TestBytes.qualifier, TestBytes.value2)
          .add(TestBytes.family2, TestBytes.qualifier2, TestBytes.value));
      txContext.finish();

      // verify new values appear
      txContext.start();
      result = txTable.get(new Get(TestBytes.row));
      assertFalse(result.isEmpty());
      assertEquals(2, result.size());
      assertArrayEquals(TestBytes.value2, result.getValue(TestBytes.family, TestBytes.qualifier));
      assertArrayEquals(TestBytes.value, result.getValue(TestBytes.family2, TestBytes.qualifier2));

      scanner = txTable.getScanner(new Scan(TestBytes.row));
      Result result1 = scanner.next();
      assertNotNull(result1);
      assertFalse(result1.isEmpty());
      assertEquals(2, result1.size());
      assertArrayEquals(TestBytes.value2, result.getValue(TestBytes.family, TestBytes.qualifier));
      assertArrayEquals(TestBytes.value, result.getValue(TestBytes.family2, TestBytes.qualifier2));
      scanner.close();
      txContext.finish();

      // Test 2: delete of first column family
      txContext.start();
      txTable.put(new Put(TestBytes.row2)
          .add(TestBytes.family, TestBytes.qualifier, TestBytes.value)
          .add(TestBytes.family, TestBytes.qualifier2, TestBytes.value2)
          .add(TestBytes.family2, TestBytes.qualifier, TestBytes.value)
          .add(TestBytes.family2, TestBytes.qualifier2, TestBytes.value2));
      txContext.finish();

      txContext.start();
      txTable.delete(new Delete(TestBytes.row2).deleteFamily(TestBytes.family));
      txContext.finish();

      txContext.start();
      Result fam1Result = txTable.get(new Get(TestBytes.row2));
      assertFalse(fam1Result.isEmpty());
      assertEquals(2, fam1Result.size());
      assertArrayEquals(TestBytes.value, fam1Result.getValue(TestBytes.family2, TestBytes.qualifier));
      assertArrayEquals(TestBytes.value2, fam1Result.getValue(TestBytes.family2, TestBytes.qualifier2));
      txContext.finish();

      // Test 3: delete of second column family
      txContext.start();
      txTable.put(new Put(TestBytes.row3)
          .add(TestBytes.family, TestBytes.qualifier, TestBytes.value)
          .add(TestBytes.family, TestBytes.qualifier2, TestBytes.value2)
          .add(TestBytes.family2, TestBytes.qualifier, TestBytes.value)
          .add(TestBytes.family2, TestBytes.qualifier2, TestBytes.value2));
      txContext.finish();

      txContext.start();
      txTable.delete(new Delete(TestBytes.row3).deleteFamily(TestBytes.family2));
      txContext.finish();

      txContext.start();
      Result fam2Result = txTable.get(new Get(TestBytes.row3));
      assertFalse(fam2Result.isEmpty());
      assertEquals(2, fam2Result.size());
      assertArrayEquals(TestBytes.value, fam2Result.getValue(TestBytes.family, TestBytes.qualifier));
      assertArrayEquals(TestBytes.value2, fam2Result.getValue(TestBytes.family, TestBytes.qualifier2));
      txContext.finish();

      // Test 4: delete specific rows in a range
      txContext.start();
      for (int i = 0; i < 10; i++) {
        txTable.put(new Put(Bytes.toBytes("z" + i))
            .add(TestBytes.family, TestBytes.qualifier, Bytes.toBytes(i))
            .add(TestBytes.family2, TestBytes.qualifier2, Bytes.toBytes(i)));
      }
      txContext.finish();

      txContext.start();
      // delete odd rows
      for (int i = 1; i < 10; i += 2) {
        txTable.delete(new Delete(Bytes.toBytes("z" + i)));
      }
      txContext.finish();

      txContext.start();
      int cnt = 0;
      ResultScanner zScanner = txTable.getScanner(new Scan(Bytes.toBytes("z0")));
      Result res;
      while ((res = zScanner.next()) != null) {
        assertFalse(res.isEmpty());
        assertArrayEquals(Bytes.toBytes("z" + cnt), res.getRow());
        assertArrayEquals(Bytes.toBytes(cnt), res.getValue(TestBytes.family, TestBytes.qualifier));
        assertArrayEquals(Bytes.toBytes(cnt), res.getValue(TestBytes.family2, TestBytes.qualifier2));
        cnt += 2;
      }

      // Test 5: delete prior writes in the same transaction
      txContext.start();
      txTable.put(new Put(TestBytes.row4)
          .add(TestBytes.family, TestBytes.qualifier, TestBytes.value)
          .add(TestBytes.family2, TestBytes.qualifier2, TestBytes.value2));
      txTable.delete(new Delete(TestBytes.row4));
      txContext.finish();

      txContext.start();
      Result row4Result = txTable.get(new Get(TestBytes.row4));
      assertTrue(row4Result.isEmpty());
      txContext.finish();
    } finally {
      txTable.close();
    }
  }

  /**
   * Expect an exception since a transaction hasn't been started.
   *
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
    Assert.assertNotNull(col1Value);
    Assert.assertArrayEquals(value, col1Value);
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

  @Test
  public void testNoneLevelConflictDetection() throws Exception {
    InMemoryTxSystemClient txClient = new InMemoryTxSystemClient(txManager);
    TransactionAwareHTable txTable1 = new TransactionAwareHTable(new HTable(conf, TestBytes.table),
        TxConstants.ConflictDetection.NONE);
    TransactionContext txContext1 = new TransactionContext(txClient, txTable1);

    TransactionAwareHTable txTable2 = new TransactionAwareHTable(new HTable(conf, TestBytes.table),
        TxConstants.ConflictDetection.NONE);
    TransactionContext txContext2 = new TransactionContext(txClient, txTable2);

    // overlapping writes to the same row + column should not conflict

    txContext1.start();
    txTable1.put(new Put(TestBytes.row).add(TestBytes.family, TestBytes.qualifier, TestBytes.value));

    // changes should not be visible yet
    txContext2.start();
    Result row = txTable2.get(new Get(TestBytes.row));
    assertTrue(row.isEmpty());

    txTable2.put(new Put(TestBytes.row).add(TestBytes.family, TestBytes.qualifier, TestBytes.value2));

    // both commits should succeed
    txContext1.finish();
    txContext2.finish();

    txContext1.start();
    row = txTable1.get(new Get(TestBytes.row));
    assertFalse(row.isEmpty());
    assertArrayEquals(TestBytes.value2, row.getValue(TestBytes.family, TestBytes.qualifier));
    txContext1.finish();

    // transaction abort should still rollback changes

    txContext1.start();
    txTable1.put(new Put(TestBytes.row2).add(TestBytes.family, TestBytes.qualifier, TestBytes.value));
    txContext1.abort();

    // changes to row2 should be rolled back
    txContext2.start();
    Result row2 = txTable2.get(new Get(TestBytes.row2));
    assertTrue(row2.isEmpty());
    txContext2.finish();

    // transaction invalidate should still make changes invisible

    txContext1.start();
    Transaction tx1 = txContext1.getCurrentTransaction();
    txTable1.put(new Put(TestBytes.row3).add(TestBytes.family, TestBytes.qualifier, TestBytes.value));
    assertNotNull(tx1);
    txClient.invalidate(tx1.getWritePointer());

    // changes to row2 should be rolled back
    txContext2.start();
    Result row3 = txTable2.get(new Get(TestBytes.row3));
    assertTrue(row3.isEmpty());
    txContext2.finish();
  }

  @Test
  public void testCheckpoint() throws Exception {
    // start a transaction, using checkpoints between writes
    transactionContext.start();
    transactionAwareHTable.put(new Put(TestBytes.row).add(TestBytes.family, TestBytes.qualifier, TestBytes.value));
    Transaction origTx = transactionContext.getCurrentTransaction();
    transactionContext.checkpoint();
    Transaction postCheckpointTx = transactionContext.getCurrentTransaction();

    assertEquals(origTx.getTransactionId(), postCheckpointTx.getTransactionId());
    assertNotEquals(origTx.getWritePointer(), postCheckpointTx.getWritePointer());
    long[] checkpointPtrs = postCheckpointTx.getCheckpointWritePointers();
    assertEquals(1, checkpointPtrs.length);
    assertEquals(postCheckpointTx.getWritePointer(), checkpointPtrs[0]);

    transactionAwareHTable.put(new Put(TestBytes.row2).add(TestBytes.family, TestBytes.qualifier, TestBytes.value2));
    transactionContext.checkpoint();
    Transaction postCheckpointTx2 = transactionContext.getCurrentTransaction();

    assertEquals(origTx.getTransactionId(), postCheckpointTx2.getTransactionId());
    assertNotEquals(postCheckpointTx.getWritePointer(), postCheckpointTx2.getWritePointer());
    long[] checkpointPtrs2 = postCheckpointTx2.getCheckpointWritePointers();
    assertEquals(2, checkpointPtrs2.length);
    assertEquals(postCheckpointTx.getWritePointer(), checkpointPtrs2[0]);
    assertEquals(postCheckpointTx2.getWritePointer(), checkpointPtrs2[1]);

    transactionAwareHTable.put(new Put(TestBytes.row3).add(TestBytes.family, TestBytes.qualifier, TestBytes.value));

    // by default, all rows should be visible with Read-Your-Writes
    verifyRow(transactionAwareHTable, TestBytes.row, TestBytes.value);
    verifyRow(transactionAwareHTable, TestBytes.row2, TestBytes.value2);
    verifyRow(transactionAwareHTable, TestBytes.row3, TestBytes.value);

    // when disabling current write pointer, only the previous checkpoints should be visible
    transactionContext.getCurrentTransaction().setVisibility(Transaction.VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
    Get get = new Get(TestBytes.row);
    verifyRow(transactionAwareHTable, get, TestBytes.value);
    get = new Get(TestBytes.row2);
    verifyRow(transactionAwareHTable, get, TestBytes.value2);
    get = new Get(TestBytes.row3);
    verifyRow(transactionAwareHTable, get, null);

    // test scan results excluding current write pointer
    Scan scan = new Scan();
    ResultScanner scanner = transactionAwareHTable.getScanner(scan);

    Result row = scanner.next();
    assertNotNull(row);
    assertArrayEquals(TestBytes.row, row.getRow());
    assertEquals(1, row.size());
    assertArrayEquals(TestBytes.value, row.getValue(TestBytes.family, TestBytes.qualifier));

    row = scanner.next();
    assertNotNull(row);
    assertArrayEquals(TestBytes.row2, row.getRow());
    assertEquals(1, row.size());
    assertArrayEquals(TestBytes.value2, row.getValue(TestBytes.family, TestBytes.qualifier));

    row = scanner.next();
    assertNull(row);
    scanner.close();
    transactionContext.getCurrentTransaction().setVisibility(Transaction.VisibilityLevel.SNAPSHOT);

    // commit transaction, verify writes are visible
    transactionContext.finish();

    transactionContext.start();
    verifyRow(transactionAwareHTable, TestBytes.row, TestBytes.value);
    verifyRow(transactionAwareHTable, TestBytes.row2, TestBytes.value2);
    verifyRow(transactionAwareHTable, TestBytes.row3, TestBytes.value);
    transactionContext.finish();
  }

  @Test
  public void testInProgressCheckpoint() throws Exception {
    // start a transaction, using checkpoints between writes
    transactionContext.start();
    transactionAwareHTable.put(new Put(TestBytes.row).add(TestBytes.family, TestBytes.qualifier, TestBytes.value));
    transactionContext.checkpoint();
    transactionAwareHTable.put(new Put(TestBytes.row2).add(TestBytes.family, TestBytes.qualifier, TestBytes.value2));

    // check that writes are still not visible to other clients
    TransactionAwareHTable txTable2 = new TransactionAwareHTable(new HTable(conf, TestBytes.table));
    TransactionContext txContext2 = new TransactionContext(new InMemoryTxSystemClient(txManager), txTable2);

    txContext2.start();
    verifyRow(txTable2, TestBytes.row, null);
    verifyRow(txTable2, TestBytes.row2, null);
    txContext2.finish();
    txTable2.close();

    transactionContext.finish();

    // verify writes are visible after commit
    transactionContext.start();
    verifyRow(transactionAwareHTable, TestBytes.row, TestBytes.value);
    verifyRow(transactionAwareHTable, TestBytes.row2, TestBytes.value2);
    transactionContext.finish();
  }

  @Test
  public void testCheckpointRollback() throws Exception {
    // start a transaction, using checkpoints between writes
    transactionContext.start();
    transactionAwareHTable.put(new Put(TestBytes.row).add(TestBytes.family, TestBytes.qualifier, TestBytes.value));
    transactionContext.checkpoint();
    transactionAwareHTable.put(new Put(TestBytes.row2).add(TestBytes.family, TestBytes.qualifier, TestBytes.value2));
    transactionContext.checkpoint();
    transactionAwareHTable.put(new Put(TestBytes.row3).add(TestBytes.family, TestBytes.qualifier, TestBytes.value));

    transactionContext.abort();

    transactionContext.start();
    verifyRow(transactionAwareHTable, TestBytes.row, null);
    verifyRow(transactionAwareHTable, TestBytes.row2, null);
    verifyRow(transactionAwareHTable, TestBytes.row3, null);

    Scan scan = new Scan();
    ResultScanner scanner = transactionAwareHTable.getScanner(scan);
    assertNull(scanner.next());
    scanner.close();
    transactionContext.finish();
  }

  @Test
  public void testCheckpointInvalidate() throws Exception {
    // start a transaction, using checkpoints between writes
    transactionContext.start();
    Transaction origTx = transactionContext.getCurrentTransaction();
    transactionAwareHTable.put(new Put(TestBytes.row).add(TestBytes.family, TestBytes.qualifier, TestBytes.value));
    transactionContext.checkpoint();
    Transaction checkpointTx1 = transactionContext.getCurrentTransaction();
    transactionAwareHTable.put(new Put(TestBytes.row2).add(TestBytes.family, TestBytes.qualifier, TestBytes.value2));
    transactionContext.checkpoint();
    Transaction checkpointTx2 = transactionContext.getCurrentTransaction();
    transactionAwareHTable.put(new Put(TestBytes.row3).add(TestBytes.family, TestBytes.qualifier, TestBytes.value));

    TransactionSystemClient txClient = new InMemoryTxSystemClient(txManager);
    txClient.invalidate(transactionContext.getCurrentTransaction().getTransactionId());

    // check that writes are not visible
    TransactionAwareHTable txTable2 = new TransactionAwareHTable(new HTable(conf, TestBytes.table));
    TransactionContext txContext2 = new TransactionContext(txClient, txTable2);
    txContext2.start();
    Transaction newTx = txContext2.getCurrentTransaction();

    // all 3 writes pointers from the previous transaction should now be excluded
    assertTrue(newTx.isExcluded(origTx.getWritePointer()));
    assertTrue(newTx.isExcluded(checkpointTx1.getWritePointer()));
    assertTrue(newTx.isExcluded(checkpointTx2.getWritePointer()));

    verifyRow(txTable2, TestBytes.row, null);
    verifyRow(txTable2, TestBytes.row2, null);
    verifyRow(txTable2, TestBytes.row3, null);

    Scan scan = new Scan();
    ResultScanner scanner = txTable2.getScanner(scan);
    assertNull(scanner.next());
    scanner.close();
    txContext2.finish();
  }

  @Test
  public void testExistingData() throws Exception {
    byte[] val11 = Bytes.toBytes("val11");
    byte[] val12 = Bytes.toBytes("val12");
    byte[] val21 = Bytes.toBytes("val21");
    byte[] val22 = Bytes.toBytes("val22");
    byte[] val31 = Bytes.toBytes("val31");
    byte[] val111 = Bytes.toBytes("val111");

    TransactionAwareHTable txTable =
      new TransactionAwareHTable(createTable(Bytes.toBytes("testExistingData"), new byte[][]{TestBytes.family}, true));
    TransactionContext txContext = new TransactionContext(new InMemoryTxSystemClient(txManager), txTable);

    // Add some pre-existing, non-transactional data
    HTable nonTxTable = new HTable(testUtil.getConfiguration(), txTable.getTableName());
    nonTxTable.put(new Put(TestBytes.row).add(TestBytes.family, TestBytes.qualifier, val11));
    nonTxTable.put(new Put(TestBytes.row).add(TestBytes.family, TestBytes.qualifier2, val12));
    nonTxTable.put(new Put(TestBytes.row2).add(TestBytes.family, TestBytes.qualifier, val21));
    nonTxTable.put(new Put(TestBytes.row2).add(TestBytes.family, TestBytes.qualifier2, val22));
    nonTxTable.flushCommits();

    // Add transactional data
    txContext.start();
    txTable.put(new Put(TestBytes.row3).add(TestBytes.family, TestBytes.qualifier, val31));
    txContext.finish();

    txContext.start();
    // test get
    verifyRow(txTable, new Get(TestBytes.row).addColumn(TestBytes.family, TestBytes.qualifier), val11);
    verifyRow(txTable, new Get(TestBytes.row).addColumn(TestBytes.family, TestBytes.qualifier2), val12);
    verifyRow(txTable, new Get(TestBytes.row2).addColumn(TestBytes.family, TestBytes.qualifier), val21);
    verifyRow(txTable, new Get(TestBytes.row2).addColumn(TestBytes.family, TestBytes.qualifier2), val22);
    verifyRow(txTable, new Get(TestBytes.row3).addColumn(TestBytes.family, TestBytes.qualifier), val31);

    // test scan
    try (ResultScanner scanner = txTable.getScanner(new Scan())) {
      Result result = scanner.next();
      assertNotNull(result);
      assertArrayEquals(val11, result.getValue(TestBytes.family, TestBytes.qualifier));
      assertArrayEquals(val12, result.getValue(TestBytes.family, TestBytes.qualifier2));
      result = scanner.next();
      assertNotNull(result);
      assertArrayEquals(val21, result.getValue(TestBytes.family, TestBytes.qualifier));
      assertArrayEquals(val22, result.getValue(TestBytes.family, TestBytes.qualifier2));
      result = scanner.next();
      assertNotNull(result);
      assertArrayEquals(val31, result.getValue(TestBytes.family, TestBytes.qualifier));
      assertNull(scanner.next());
    }
    txContext.finish();

    // test update and delete
    txContext.start();
    txTable.put(new Put(TestBytes.row).add(TestBytes.family, TestBytes.qualifier, val111));
    txTable.delete(new Delete(TestBytes.row2).deleteColumns(TestBytes.family, TestBytes.qualifier));
    txContext.finish();

    txContext.start();
    verifyRow(txTable, new Get(TestBytes.row).addColumn(TestBytes.family, TestBytes.qualifier), val111);
    verifyRow(txTable, new Get(TestBytes.row).addColumn(TestBytes.family, TestBytes.qualifier2), val12);
    verifyRow(txTable, new Get(TestBytes.row2).addColumn(TestBytes.family, TestBytes.qualifier), null);
    verifyRow(txTable, new Get(TestBytes.row2).addColumn(TestBytes.family, TestBytes.qualifier2), val22);
    verifyRow(txTable, new Get(TestBytes.row3).addColumn(TestBytes.family, TestBytes.qualifier), val31);
    txContext.finish();

    // test scan
    txContext.start();
    try (ResultScanner scanner = txTable.getScanner(new Scan())) {
      Result result = scanner.next();
      assertNotNull(result);
      assertArrayEquals(val111, result.getValue(TestBytes.family, TestBytes.qualifier));
      assertArrayEquals(val12, result.getValue(TestBytes.family, TestBytes.qualifier2));
      result = scanner.next();
      assertNotNull(result);
      assertArrayEquals(null, result.getValue(TestBytes.family, TestBytes.qualifier));
      assertArrayEquals(val22, result.getValue(TestBytes.family, TestBytes.qualifier2));
      result = scanner.next();
      assertNotNull(result);
      assertArrayEquals(val31, result.getValue(TestBytes.family, TestBytes.qualifier));
      assertNull(scanner.next());
    }
    txContext.finish();
  }

  private void verifyRow(HTableInterface table, byte[] rowkey, byte[] expectedValue) throws Exception {
    verifyRow(table, new Get(rowkey), expectedValue);
  }

  private void verifyRow(HTableInterface table, Get get, byte[] expectedValue) throws Exception {
    Result result = table.get(get);
    if (expectedValue == null) {
      assertTrue(result.isEmpty());
    } else {
      assertFalse(result.isEmpty());
      if (get.hasFamilies()) {
        byte[] family = get.getFamilyMap().keySet().iterator().next();
        byte[] col = get.getFamilyMap().get(family).first();
        assertArrayEquals(expectedValue, result.getValue(family, col));
      } else {
        assertArrayEquals(expectedValue, result.getValue(TestBytes.family, TestBytes.qualifier));
      }
    }
  }
}
