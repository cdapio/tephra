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
package co.cask.tephra.hbase98;

import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.hbase98.coprocessor.TransactionProcessor;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import co.cask.tephra.metrics.TxMetricsCollector;
import co.cask.tephra.persist.InMemoryTransactionStateStorage;
import co.cask.tephra.persist.TransactionStateStorage;
import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;

/**
 * Tests for TransactionAwareHTables.
 */
public class TransactionAwareHTableTest {
  private static HBaseTestingUtility testUtil;
  private static HBaseAdmin hBaseAdmin;
  private static TransactionStateStorage txStateStorage;
  private static TransactionManager txManager;
  private static Configuration conf;
  private TransactionContext transactionContext;
  private TransactionAwareHTable transactionAwareHTable;

  private static final class TestBytes {
    private static final byte[] table = Bytes.toBytes("testtable");
    private static final byte[] family = Bytes.toBytes("testfamily");
    private static final byte[] qualifier = Bytes.toBytes("testqualifier");
    private static final byte[] row = Bytes.toBytes("testrow");
    private static final byte[] value = Bytes.toBytes("testvalue");
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
    HTable hTable = createTable(TestBytes.table, TestBytes.family);
    transactionAwareHTable = new TransactionAwareHTable(hTable);
    transactionContext = new TransactionContext(new InMemoryTxSystemClient(txManager), transactionAwareHTable);
  }

  @After
  public void shutdownAfterTest() throws IOException {
    hBaseAdmin.disableTable(TestBytes.table);
    hBaseAdmin.deleteTable(TestBytes.table);
  }

  private HTable createTable(byte[] tableName, byte[] columnFamily) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor columnDesc = new HColumnDescriptor(columnFamily);
    columnDesc.setMaxVersions(Integer.MAX_VALUE);
    desc.addFamily(columnDesc);
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
    Assert.assertArrayEquals(TestBytes.value, value);
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
    Assert.assertArrayEquals(value, null);
  }

  /**
   * Test transactional delete operations.
   * @throws Exception
   */
  @Test
  public void testValidTransactionalDelete() throws Exception {
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    transactionAwareHTable.put(put);
    transactionContext.finish();

    transactionContext.start();
    Result result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(TestBytes.value, value);

    transactionContext.start();
    Delete delete = new Delete(TestBytes.row);
    transactionAwareHTable.delete(delete);

    transactionContext.finish();

    transactionContext.start();
    result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertNull(value);
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
    Assert.assertArrayEquals(TestBytes.value, value);

    transactionContext.start();
    Delete delete = new Delete(TestBytes.row);
    transactionAwareHTable.delete(delete);
    transactionContext.abort();

    transactionContext.start();
    result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(TestBytes.value, value);
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
    Assert.assertFalse(row.isEmpty());
    byte[] col1Value = row.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertNotNull(col1Value);
    Assert.assertArrayEquals(value, col1Value);
    // write from in-progress transaction should not be visible
    byte[] col2Value = row.getValue(TestBytes.family, col2);
    Assert.assertNull(col2Value);

    // commit in-progress transaction, should still not be visible
    inprogressTxContext1.finish();

    get = new Get(TestBytes.row);
    row = transactionAwareHTable.get(get);
    Assert.assertFalse(row.isEmpty());
    col2Value = row.getValue(TestBytes.family, col2);
    Assert.assertNull(col2Value);

    transactionContext.finish();

    inprogressTxContext2.abort();
  }
}
