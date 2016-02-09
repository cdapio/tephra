/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.tephra.hbase11.coprocessor;

import co.cask.tephra.ChangeId;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionType;
import co.cask.tephra.TxConstants;
import co.cask.tephra.coprocessor.TransactionStateCache;
import co.cask.tephra.coprocessor.TransactionStateCacheSupplier;
import co.cask.tephra.metrics.TxMetricsCollector;
import co.cask.tephra.persist.HDFSTransactionStateStorage;
import co.cask.tephra.persist.TransactionSnapshot;
import co.cask.tephra.persist.TransactionVisibilityState;
import co.cask.tephra.snapshot.DefaultSnapshotCodec;
import co.cask.tephra.snapshot.SnapshotCodecProvider;
import co.cask.tephra.util.TxUtils;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MockRegionServerServices;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests filtering of invalid transaction data by the {@link TransactionProcessor} coprocessor.
 */
public class TransactionProcessorTest {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionProcessorTest.class);

  // 8 versions, 1 hour apart, latest is current ts.
  private static final long[] V;

  static {
    long now = System.currentTimeMillis();
    V = new long[9];
    for (int i = 0; i < V.length; i++) {
      V[i] = (now - TimeUnit.HOURS.toMillis(8 - i)) * TxConstants.MAX_TX_PER_MS;
    }
  }

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  private static MiniDFSCluster dfsCluster;
  private static Configuration conf;
  private static LongArrayList invalidSet = new LongArrayList(new long[]{V[3], V[5], V[7]});
  private static TransactionVisibilityState txVisibilityState;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration hConf = new Configuration();
    String rootDir = tmpFolder.newFolder().getAbsolutePath();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, rootDir);
    hConf.set(HConstants.HBASE_DIR, rootDir + "/hbase");

    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    dfsCluster.waitActive();
    conf = HBaseConfiguration.create(dfsCluster.getFileSystem().getConf());

    conf.unset(TxConstants.Manager.CFG_TX_HDFS_USER);
    conf.unset(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES);
    String localTestDir = tmpFolder.newFolder().getAbsolutePath();
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, localTestDir);
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, DefaultSnapshotCodec.class.getName());

    // write an initial transaction snapshot
    TransactionSnapshot txSnapshot = TransactionSnapshot.copyFrom(
        System.currentTimeMillis(), V[6] - 1, V[7], invalidSet,
        // this will set visibility upper bound to V[6]
        Maps.newTreeMap(ImmutableSortedMap.of(V[6], new TransactionManager.InProgressTx(V[6] - 1, Long.MAX_VALUE,
                                                                                        TransactionType.SHORT))),
        new HashMap<Long, Set<ChangeId>>(), new TreeMap<Long, Set<ChangeId>>());
    txVisibilityState = new TransactionSnapshot(txSnapshot.getTimestamp(), txSnapshot.getReadPointer(),
                                                txSnapshot.getWritePointer(), txSnapshot.getInvalid(),
                                                txSnapshot.getInProgress());
    HDFSTransactionStateStorage tmpStorage =
      new HDFSTransactionStateStorage(conf, new SnapshotCodecProvider(conf), new TxMetricsCollector());
    tmpStorage.startAndWait();
    tmpStorage.writeSnapshot(txSnapshot);
    tmpStorage.stopAndWait();
  }

  @AfterClass
  public static void shutdownAfterClass() throws Exception {
    dfsCluster.shutdown();
  }

  @Test
  public void testDataJanitorRegionScanner() throws Exception {
    String tableName = "TestRegionScanner";
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    HRegion region = createRegion(tableName, familyBytes, TimeUnit.HOURS.toMillis(3));
    try {
      region.initialize();
      TransactionStateCache cache = new TransactionStateCacheSupplier(conf).get();
      LOG.info("Coprocessor is using transaction state: " + cache.getLatestState());

      for (int i = 1; i <= 8; i++) {
        for (int k = 1; k <= i; k++) {
          Put p = new Put(Bytes.toBytes(i));
          p.add(familyBytes, columnBytes, V[k], Bytes.toBytes(V[k]));
          region.put(p);
        }
      }

      List<Cell> results = Lists.newArrayList();

      // force a flush to clear the data
      // during flush, the coprocessor should drop all KeyValues with timestamps in the invalid set

      LOG.info("Flushing region " + region.getRegionInfo().getRegionNameAsString());
      region.flushcache(true, false);

      // now a normal scan should only return the valid rows
      // do not use a filter here to test that cleanup works on flush
      Scan scan = new Scan();
      scan.setMaxVersions(10);
      RegionScanner regionScanner = region.getScanner(scan);

      // first returned value should be "4" with version "4"
      results.clear();
      assertTrue(regionScanner.next(results));
      assertKeyValueMatches(results, 4, new long[]{V[4]});

      results.clear();
      assertTrue(regionScanner.next(results));
      assertKeyValueMatches(results, 5, new long[] {V[4]});

      results.clear();
      assertTrue(regionScanner.next(results));
      assertKeyValueMatches(results, 6, new long[]{V[6], V[4]});

      results.clear();
      assertTrue(regionScanner.next(results));
      assertKeyValueMatches(results, 7, new long[]{V[6], V[4]});

      results.clear();
      assertFalse(regionScanner.next(results));
      assertKeyValueMatches(results, 8, new long[] {V[8], V[6], V[4]});
    } finally {
      region.close();
    }
  }

  @Test
  public void testDeleteFiltering() throws Exception {
    String tableName = "TestDeleteFiltering";
    byte[] familyBytes = Bytes.toBytes("f");
    byte[] columnBytes = Bytes.toBytes("c");
    HRegion region = createRegion(tableName, familyBytes, 0);
    try {
      region.initialize();
      TransactionStateCache cache = new TransactionStateCacheSupplier(conf).get();
      LOG.info("Coprocessor is using transaction state: " + cache.getLatestState());

      byte[] row = Bytes.toBytes(1);
      for (int i = 4; i < V.length; i++) {
        Put p = new Put(row);
        p.add(familyBytes, columnBytes, V[i], Bytes.toBytes(V[i]));
        region.put(p);
      }

      // delete from the third entry back
      // take that cell's timestamp + 1 to simulate a delete in a new tx
      long deleteTs = V[5] + 1;
      Delete d = new Delete(row, deleteTs);
      LOG.info("Issuing delete at timestamp " + deleteTs);
      // row deletes are not yet supported (TransactionAwareHTable normally handles this)
      d.deleteColumns(familyBytes, columnBytes);
      region.delete(d);

      List<Cell> results = Lists.newArrayList();

      // force a flush to clear the data
      // during flush, we should drop the deleted version, but not the others
      LOG.info("Flushing region " + region.getRegionInfo().getRegionNameAsString());
      region.flushcache(true, false);

      // now a normal scan should return row with versions at: V[8], V[6].
      // V[7] is invalid and V[5] and prior are deleted.
      Scan scan = new Scan();
      scan.setMaxVersions(10);
      RegionScanner regionScanner = region.getScanner(scan);
      // should be only one row
      assertFalse(regionScanner.next(results));
      assertKeyValueMatches(results, 1,
          new long[]{V[8], V[6], deleteTs},
          new byte[][]{Bytes.toBytes(V[8]), Bytes.toBytes(V[6]), new byte[0]});
    } finally {
      region.close();
    }
  }

  @Test
  public void testDeleteMarkerCleanup() throws Exception {
    String tableName = "TestDeleteMarkerCleanup";
    byte[] familyBytes = Bytes.toBytes("f");
    HRegion region = createRegion(tableName, familyBytes, 0);
    try {
      region.initialize();

      // all puts use a timestamp before the tx snapshot's visibility upper bound, making them eligible for removal
      long writeTs = txVisibilityState.getVisibilityUpperBound() - 10;
      // deletes are performed after the writes, but still before the visibility upper bound
      long deleteTs = writeTs + 1;
      // write separate columns to confirm that delete markers survive across flushes
      byte[] row = Bytes.toBytes(100);
      Put p = new Put(row);

      LOG.info("Writing columns at timestamp " + writeTs);
      for (int i = 0; i < 5; i++) {
        byte[] iBytes = Bytes.toBytes(i);
        p.add(familyBytes, iBytes, writeTs, iBytes);
      }
      region.put(p);
      // read all back
      Scan scan = new Scan(row);
      RegionScanner regionScanner = region.getScanner(scan);
      List<Cell> results = Lists.newArrayList();
      assertFalse(regionScanner.next(results));
      for (int i = 0; i < 5; i++) {
        Cell cell = results.get(i);
        assertArrayEquals(row, cell.getRow());
        byte[] idxBytes = Bytes.toBytes(i);
        assertArrayEquals(idxBytes, cell.getQualifier());
        assertArrayEquals(idxBytes, cell.getValue());
      }

      // force a flush to clear the memstore
      LOG.info("Before delete, flushing region " + region.getRegionInfo().getRegionNameAsString());
      region.flushcache(false, false);

      // delete the odd entries
      for (int i = 0; i < 5; i++) {
        if (i % 2 == 1) {
          // deletes are performed as puts with empty values
          Put deletePut = new Put(row);
          deletePut.add(familyBytes, Bytes.toBytes(i), deleteTs, new byte[0]);
          region.put(deletePut);
        }
      }

      // read all back
      scan = new Scan(row);
      scan.setFilter(TransactionFilters.getVisibilityFilter(TxUtils.createDummyTransaction(txVisibilityState),
                                                            new TreeMap<byte[], Long>(), false, ScanType.USER_SCAN));
      regionScanner = region.getScanner(scan);
      results = Lists.newArrayList();
      assertFalse(regionScanner.next(results));
      assertEquals(3, results.size());
      // only even columns should exist
      for (int i = 0; i < 3; i++) {
        Cell cell = results.get(i);
        LOG.info("Got cell " + cell);
        assertArrayEquals(row, cell.getRow());
        byte[] idxBytes = Bytes.toBytes(i * 2);
        assertArrayEquals(idxBytes, cell.getQualifier());
        assertArrayEquals(idxBytes, cell.getValue());
      }

      // force another flush on the delete markers
      // during flush, we should retain the delete markers, since they can only safely be dropped by a major compaction
      LOG.info("After delete, flushing region " + region.getRegionInfo().getRegionNameAsString());
      region.flushcache(true, false);

      scan = new Scan(row);
      scan.setFilter(TransactionFilters.getVisibilityFilter(TxUtils.createDummyTransaction(txVisibilityState),
                                                            new TreeMap<byte[], Long>(), false, ScanType.USER_SCAN));

      regionScanner = region.getScanner(scan);
      results = Lists.newArrayList();
      assertFalse(regionScanner.next(results));
      assertEquals(3, results.size());
      // only even columns should exist
      for (int i = 0; i < 3; i++) {
        Cell cell = results.get(i);
        assertArrayEquals(row, cell.getRow());
        byte[] idxBytes = Bytes.toBytes(i * 2);
        assertArrayEquals(idxBytes, cell.getQualifier());
        assertArrayEquals(idxBytes, cell.getValue());
      }

      // force a major compaction
      LOG.info("Forcing major compaction of region " + region.getRegionInfo().getRegionNameAsString());
      region.compact(true);

      // perform a raw scan (no filter) to confirm that the delete markers are now gone
      scan = new Scan(row);
      regionScanner = region.getScanner(scan);
      results = Lists.newArrayList();
      assertFalse(regionScanner.next(results));
      assertEquals(3, results.size());
      // only even columns should exist
      for (int i = 0; i < 3; i++) {
        Cell cell = results.get(i);
        assertArrayEquals(row, cell.getRow());
        byte[] idxBytes = Bytes.toBytes(i * 2);
        assertArrayEquals(idxBytes, cell.getQualifier());
        assertArrayEquals(idxBytes, cell.getValue());
      }
    } finally {
      region.close();
    }
  }

  /**
   * Test that we correctly preserve the timestamp set for column family delete markers.  This is not
   * directly required for the TransactionAwareHTable usage, but is the right thing to do and ensures
   * that we make it easy to interoperate with other systems.
   */
  @Test
  public void testFamilyDeleteTimestamp() throws Exception {
    String tableName = "TestFamilyDeleteTimestamp";
    byte[] family1Bytes = Bytes.toBytes("f1");
    byte[] columnBytes = Bytes.toBytes("c");
    byte[] rowBytes = Bytes.toBytes("row");
    byte[] valBytes = Bytes.toBytes("val");
    HRegion region = createRegion(tableName, family1Bytes, 0);
    try {
      region.initialize();

      long now = System.currentTimeMillis() * TxConstants.MAX_TX_PER_MS;
      Put p = new Put(rowBytes);
      p.add(family1Bytes, columnBytes, now - 10, valBytes);
      region.put(p);

      // issue a family delete with an explicit timestamp
      Delete delete = new Delete(rowBytes, now);
      delete.deleteFamily(family1Bytes, now - 5);
      region.delete(delete);

      // test that the delete marker preserved the timestamp
      Scan scan = new Scan();
      scan.setMaxVersions();
      RegionScanner scanner = region.getScanner(scan);
      List<Cell> results = Lists.newArrayList();
      scanner.next(results);
      assertEquals(2, results.size());
      // delete marker should appear first
      Cell cell = results.get(0);
      assertArrayEquals(new byte[0], cell.getQualifier());
      assertArrayEquals(new byte[0], cell.getValue());
      assertEquals(now - 5, cell.getTimestamp());
      // since this is an unfiltered scan against the region, the original put should be next
      cell = results.get(1);
      assertArrayEquals(valBytes, cell.getValue());
      assertEquals(now - 10, cell.getTimestamp());
      scanner.close();


      // with a filtered scan the original put should disappear
      scan = new Scan();
      scan.setMaxVersions();
      scan.setFilter(TransactionFilters.getVisibilityFilter(TxUtils.createDummyTransaction(txVisibilityState),
                                                            new TreeMap<byte[], Long>(), false, ScanType.USER_SCAN));
      scanner = region.getScanner(scan);
      results = Lists.newArrayList();
      scanner.next(results);
      assertEquals(0, results.size());
      scanner.close();
    } finally {
      region.close();
    }
  }

  @Test
  public void testPreExistingData() throws Exception {
    String tableName = "TestPreExistingData";
    byte[] familyBytes = Bytes.toBytes("f");
    long ttlMillis = TimeUnit.DAYS.toMillis(14);
    HRegion region = createRegion(tableName, familyBytes, ttlMillis);
    try {
      region.initialize();

      // timestamps for pre-existing, non-transactional data
      long now = txVisibilityState.getVisibilityUpperBound() / TxConstants.MAX_TX_PER_MS;
      long older = now - ttlMillis / 2;
      long newer = now - ttlMillis / 3;
      // timestamps for transactional data
      long nowTx = txVisibilityState.getVisibilityUpperBound();
      long olderTx = nowTx - (ttlMillis / 2) * TxConstants.MAX_TX_PER_MS;
      long newerTx = nowTx - (ttlMillis / 3) * TxConstants.MAX_TX_PER_MS;

      Map<byte[], Long> ttls = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      ttls.put(familyBytes, ttlMillis);

      List<Cell> cells = new ArrayList<>();
      cells.add(new KeyValue(Bytes.toBytes("r1"), familyBytes, Bytes.toBytes("c1"), older, Bytes.toBytes("v11")));
      cells.add(new KeyValue(Bytes.toBytes("r1"), familyBytes, Bytes.toBytes("c2"), newer, Bytes.toBytes("v12")));
      cells.add(new KeyValue(Bytes.toBytes("r2"), familyBytes, Bytes.toBytes("c1"), older, Bytes.toBytes("v21")));
      cells.add(new KeyValue(Bytes.toBytes("r2"), familyBytes, Bytes.toBytes("c2"), newer, Bytes.toBytes("v22")));
      cells.add(new KeyValue(Bytes.toBytes("r3"), familyBytes, Bytes.toBytes("c1"), olderTx, Bytes.toBytes("v31")));
      cells.add(new KeyValue(Bytes.toBytes("r3"), familyBytes, Bytes.toBytes("c2"), newerTx, Bytes.toBytes("v32")));

      // Write non-transactional and transactional data
      for (Cell c : cells) {
        region.put(new Put(c.getRow()).add(c.getFamily(), c.getQualifier(), c.getTimestamp(), c.getValue()));
      }

      Scan rawScan = new Scan();
      rawScan.setMaxVersions();

      Transaction dummyTransaction = TxUtils.createDummyTransaction(txVisibilityState);
      Scan txScan = new Scan();
      txScan.setMaxVersions();
      txScan.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttls, dummyTransaction, true),
                          TxUtils.getMaxVisibleTimestamp(dummyTransaction));
      txScan.setFilter(TransactionFilters.getVisibilityFilter(dummyTransaction, ttls, false, ScanType.USER_SCAN));

      // read all back with raw scanner
      scanAndAssert(region, cells, rawScan);

      // read all back with transaction filter
      scanAndAssert(region, cells, txScan);

      // force a flush to clear the memstore
      region.flushcache(true, false);
      scanAndAssert(region, cells, txScan);

      // force a major compaction to remove any expired cells
      region.compact(true);
      scanAndAssert(region, cells, txScan);

      // Reduce TTL, this should make cells with timestamps older and olderTx expire
      long newTtl = ttlMillis / 2 - 1;
      region = updateTtl(region, familyBytes, newTtl);
      ttls.put(familyBytes, newTtl);
      txScan.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttls, dummyTransaction, true),
                          TxUtils.getMaxVisibleTimestamp(dummyTransaction));
      txScan.setFilter(TransactionFilters.getVisibilityFilter(dummyTransaction, ttls, false, ScanType.USER_SCAN));

      // Raw scan should still give all cells
      scanAndAssert(region, cells, rawScan);
      // However, tx scan should not return expired cells
      scanAndAssert(region, select(cells, 1, 3, 5), txScan);

      region.flushcache(true, false);
      scanAndAssert(region, cells, rawScan);

      // force a major compaction to remove any expired cells
      region.compact(true);
      // This time raw scan too should not return expired cells, as they would be dropped during major compaction
      scanAndAssert(region, select(cells, 1, 3, 5), rawScan);

      // Reduce TTL again to 1 ms, this should expire all cells
      newTtl = 1;
      region = updateTtl(region, familyBytes, newTtl);
      ttls.put(familyBytes, newTtl);
      txScan.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttls, dummyTransaction, true),
                          TxUtils.getMaxVisibleTimestamp(dummyTransaction));
      txScan.setFilter(TransactionFilters.getVisibilityFilter(dummyTransaction, ttls, false, ScanType.USER_SCAN));

      // force a major compaction to remove expired cells
      region.compact(true);
      // This time raw scan should not return any cells, as all cells have expired.
      scanAndAssert(region, Collections.<Cell>emptyList(), rawScan);
    } finally {
      region.close();
    }
  }

  private List<Cell> select(List<Cell> cells, int... indexes) {
    List<Cell> newCells = new ArrayList<>();
    for (int i : indexes) {
      newCells.add(cells.get(i));
    }
    return newCells;
  }

  @SuppressWarnings("StatementWithEmptyBody")
  private void scanAndAssert(HRegion region, List<Cell> expected, Scan scan) throws Exception {
    try (RegionScanner regionScanner = region.getScanner(scan)) {
      List<Cell> results = Lists.newArrayList();
      while (regionScanner.next(results)) { }
      assertEquals(expected, results);
    }
  }

  private HRegion updateTtl(HRegion region, byte[] family, long ttl) throws Exception {
    region.close();
    HTableDescriptor htd = region.getTableDesc();
    HColumnDescriptor cfd = htd.getFamily(family);
    if (ttl > 0) {
      cfd.setValue(TxConstants.PROPERTY_TTL, String.valueOf(ttl));
    }
    cfd.setMaxVersions(10);
    return HRegion.openHRegion(region.getRegionInfo(), htd, region.getWAL(), conf,
                               new LocalRegionServerServices(conf, ServerName.valueOf(
                                 InetAddress.getLocalHost().getHostName(), 0, System.currentTimeMillis())), null);
  }

  private HRegion createRegion(String tableName, byte[] family, long ttl) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor cfd = new HColumnDescriptor(family);
    if (ttl > 0) {
      cfd.setValue(TxConstants.PROPERTY_TTL, String.valueOf(ttl));
    }
    cfd.setMaxVersions(10);
    htd.addFamily(cfd);
    htd.addCoprocessor(TransactionProcessor.class.getName());
    Path tablePath = FSUtils.getTableDir(FSUtils.getRootDir(conf), htd.getTableName());
    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.mkdirs(tablePath));
    WALFactory walFactory = new WALFactory(conf, null, tableName + ".hlog");
    WAL hLog = walFactory.getWAL(new byte[]{1});
    HRegionInfo regionInfo = new HRegionInfo(TableName.valueOf(tableName));
    HRegionFileSystem regionFS = HRegionFileSystem.createRegionOnFileSystem(conf, fs, tablePath, regionInfo);
    return new HRegion(regionFS, hLog, conf, htd,
        new LocalRegionServerServices(conf, ServerName.valueOf(
            InetAddress.getLocalHost().getHostName(), 0, System.currentTimeMillis())));
  }

  private void assertKeyValueMatches(List<Cell> results, int index, long[] versions) {
    byte[][] values = new byte[versions.length][];
    for (int i = 0; i < versions.length; i++) {
      values[i] = Bytes.toBytes(versions[i]);
    }
    assertKeyValueMatches(results, index, versions, values);
  }

  private void assertKeyValueMatches(List<Cell> results, int index, long[] versions, byte[][] values) {
    assertEquals(versions.length, results.size());
    assertEquals(values.length, results.size());
    for (int i = 0; i < versions.length; i++) {
      Cell kv = results.get(i);
      assertArrayEquals(Bytes.toBytes(index), kv.getRow());
      assertEquals(versions[i], kv.getTimestamp());
      assertArrayEquals(values[i], kv.getValue());
    }
  }

  @Test
  public void testTransactionStateCache() throws Exception {
    TransactionStateCache cache = new TransactionStateCache();
    cache.setConf(conf);
    cache.startAndWait();
    // verify that the transaction snapshot read matches what we wrote in setupBeforeClass()
    TransactionVisibilityState cachedSnapshot = cache.getLatestState();
    assertNotNull(cachedSnapshot);
    assertEquals(invalidSet, cachedSnapshot.getInvalid());
    cache.stopAndWait();
  }

  private static class LocalRegionServerServices extends MockRegionServerServices {
    private final ServerName serverName;

    public LocalRegionServerServices(Configuration conf, ServerName serverName) {
      super(conf);
      this.serverName = serverName;
    }

    @Override
    public ServerName getServerName() {
      return serverName;
    }
  }
}
