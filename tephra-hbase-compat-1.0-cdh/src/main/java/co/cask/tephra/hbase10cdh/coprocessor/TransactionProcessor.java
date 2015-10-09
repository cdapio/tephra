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

package co.cask.tephra.hbase10cdh.coprocessor;

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionCodec;
import co.cask.tephra.TxConstants;
import co.cask.tephra.coprocessor.TransactionStateCache;
import co.cask.tephra.coprocessor.TransactionStateCacheSupplier;
import co.cask.tephra.hbase10cdh.Filters;
import co.cask.tephra.persist.TransactionVisibilityState;
import co.cask.tephra.util.TxUtils;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

/**
 * {@code org.apache.hadoop.hbase.coprocessor.RegionObserver} coprocessor that handles server-side processing
 * for transactions:
 * <ul>
 *   <li>applies filtering to exclude data from invalid and in-progress transactions</li>
 *   <li>overrides the scanner returned for flush and compaction to drop data written by invalidated transactions,
 *   or expired due to TTL.</li>
 * </ul>
 *
 * <p>In order to use this coprocessor for transactions, configure the class on any table involved in transactions,
 * or on all user tables by adding the following to hbase-site.xml:
 * {@code
 * <property>
 *   <name>hbase.coprocessor.region.classes</name>
 *   <value>TransactionProcessor</value>
 * </property>
 * }
 * </p>
 *
 * <p>HBase {@code Get} and {@code Scan} operations should have the current transaction serialized on to the operation
 * as an attribute:
 * {@code
 * Transaction t = ...;
 * Get get = new Get(...);
 * TransactionCodec codec = new TransactionCodec();
 * codec.addToOperation(get, t);
 * }
 * </p>
 */
public class TransactionProcessor extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(TransactionProcessor.class);

  private TransactionStateCache cache;
  private final TransactionCodec txCodec;
  protected Map<byte[], Long> ttlByFamily = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  protected boolean allowEmptyValues = TxConstants.ALLOW_EMPTY_VALUES_DEFAULT;

  public TransactionProcessor() {
    this.txCodec = new TransactionCodec();
  }

  /* RegionObserver implementation */

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      Supplier<TransactionStateCache> cacheSupplier = getTransactionStateCacheSupplier(env);
      this.cache = cacheSupplier.get();

      HTableDescriptor tableDesc = env.getRegion().getTableDesc();
      for (HColumnDescriptor columnDesc : tableDesc.getFamilies()) {
        String columnTTL = columnDesc.getValue(TxConstants.PROPERTY_TTL);
        long ttl = 0;
        if (columnTTL != null) {
          try {
            ttl = Long.parseLong(columnTTL);
            LOG.info("Family " + columnDesc.getNameAsString() + " has TTL of " + columnTTL);
          } catch (NumberFormatException nfe) {
            LOG.warn("Invalid TTL value configured for column family " + columnDesc.getNameAsString() +
                       ", value = " + columnTTL);
          }
        }
        ttlByFamily.put(columnDesc.getName(), ttl);
      }

      this.allowEmptyValues = env.getConfiguration().getBoolean(TxConstants.ALLOW_EMPTY_VALUES_KEY,
                                                                TxConstants.ALLOW_EMPTY_VALUES_DEFAULT);
    }
  }

  protected Supplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    return new TransactionStateCacheSupplier(env.getConfiguration());
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    // nothing to do
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
    throws IOException {
    Transaction tx = getFromOperation(get);
    if (tx != null) {
      projectFamilyDeletes(get);
      get.setMaxVersions();
      get.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx), TxUtils.getMaxVisibleTimestamp(tx));
      Filter newFilter = Filters.combine(getTransactionFilter(tx, ScanType.USER_SCAN), get.getFilter());
      get.setFilter(newFilter);
    }
  }

  @Override
  public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
                        Durability durability) throws IOException {
    // Translate deletes into our own delete tombstones
    // Since HBase deletes cannot be undone, we need to translate deletes into special puts, which allows
    // us to rollback the changes (by a real delete) if the transaction fails

    // Deletes that are part of a transaction rollback do not need special handling.
    // They will never be rolled back, so are performed as normal HBase deletes.
    if (delete.getAttribute(TxConstants.TX_ROLLBACK_ATTRIBUTE_KEY) != null) {
      return;
    }

    // Other deletes are client-initiated and need to be translated into our own tombstones
    // TODO: this should delegate to the DeleteStrategy implementation.
    Put deleteMarkers = new Put(delete.getRow(), delete.getTimeStamp());
    for (byte[] family : delete.getFamilyCellMap().keySet()) {
      List<Cell> familyCells = delete.getFamilyCellMap().get(family);
      if (isFamilyDelete(familyCells)) {
        deleteMarkers.add(family, TxConstants.FAMILY_DELETE_QUALIFIER, familyCells.get(0).getTimestamp(),
            HConstants.EMPTY_BYTE_ARRAY);
      } else {
        int cellSize = familyCells.size();
        for (int i = 0; i < cellSize; i++) {
          Cell cell = familyCells.get(i);
          deleteMarkers.add(family, CellUtil.cloneQualifier(cell), cell.getTimestamp(),
                  HConstants.EMPTY_BYTE_ARRAY);
        }
      }
    }
    e.getEnvironment().getRegion().put(deleteMarkers);
    // skip normal delete handling
    e.bypass();
  }

  private boolean isFamilyDelete(List<Cell> familyCells) {
    return familyCells.size() == 1 && CellUtil.isDeleteFamily(familyCells.get(0));
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    Transaction tx = getFromOperation(scan);
    if (tx != null) {
      projectFamilyDeletes(scan);
      scan.setMaxVersions();
      scan.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx), TxUtils.getMaxVisibleTimestamp(tx));
      Filter newFilter = Filters.combine(getTransactionFilter(tx, ScanType.USER_SCAN), scan.getFilter());
      scan.setFilter(newFilter);
    }
    return s;
  }

  /**
   * Ensures that family delete markers are present in the columns requested for any scan operation.
   * @param scan The original scan request
   * @return The modified scan request with the family delete qualifiers represented
   */
  private Scan projectFamilyDeletes(Scan scan) {
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
      NavigableSet<byte[]> columns = entry.getValue();
      // wildcard scans will automatically include the delete marker, so only need to add it when we have
      // explicit columns listed
      if (columns != null && !columns.isEmpty()) {
        scan.addColumn(entry.getKey(), TxConstants.FAMILY_DELETE_QUALIFIER);
      }
    }
    return scan;
  }

  /**
   * Ensures that family delete markers are present in the columns requested for any get operation.
   * @param get The original get request
   * @return The modified get request with the family delete qualifiers represented
   */
  private Get projectFamilyDeletes(Get get) {
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : get.getFamilyMap().entrySet()) {
      NavigableSet<byte[]> columns = entry.getValue();
      // wildcard scans will automatically include the delete marker, so only need to add it when we have
      // explicit columns listed
      if (columns != null && !columns.isEmpty()) {
        get.addColumn(entry.getKey(), TxConstants.FAMILY_DELETE_QUALIFIER);
      }
    }
    return get;
  }

  @Override
  public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                             KeyValueScanner memstoreScanner, InternalScanner scanner)
      throws IOException {
    return createStoreScanner(c.getEnvironment(), "flush", cache.getLatestState(), store,
                              Collections.singletonList(memstoreScanner), ScanType.COMPACT_RETAIN_DELETES,
                              HConstants.OLDEST_TIMESTAMP);
  }

  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s,
      CompactionRequest request)
      throws IOException {
    return createStoreScanner(c.getEnvironment(), "compaction", cache.getLatestState(), store, scanners,
                              scanType, earliestPutTs);
  }

  protected InternalScanner createStoreScanner(RegionCoprocessorEnvironment env, String action,
                                               TransactionVisibilityState snapshot, Store store,
                                               List<? extends KeyValueScanner> scanners, ScanType type,
                                               long earliestPutTs) throws IOException {
    if (snapshot == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Region " + env.getRegion().getRegionNameAsString() +
                    ", no current transaction state found, defaulting to normal " + action + " scanner");
      }
      return null;
    }

    // construct a dummy transaction from the latest snapshot
    Transaction dummyTx = TxUtils.createDummyTransaction(snapshot);
    Scan scan = new Scan();
    // need to see all versions, since we filter out excludes and applications may rely on multiple versions
    scan.setMaxVersions();
    scan.setFilter(
        new IncludeInProgressFilter(dummyTx.getVisibilityUpperBound(),
            snapshot.getInvalid(),
            getTransactionFilter(dummyTx, type)));

    return new StoreScanner(store, store.getScanInfo(), scan, scanners,
                            type, store.getSmallestReadPoint(), earliestPutTs);
  }

  private Transaction getFromOperation(OperationWithAttributes op) throws IOException {
    byte[] encoded = op.getAttribute(TxConstants.TX_OPERATION_ATTRIBUTE_KEY);
    if (encoded != null) {
      return txCodec.decode(encoded);
    }
    return null;
  }

  /**
   * Derived classes can override this method to customize the filter used to return data visible for the current
   * transaction.
   *
   * @param tx the current transaction to apply
   * @param type the type of scan being performed
   */
  protected Filter getTransactionFilter(Transaction tx, ScanType type) {
    return new TransactionVisibilityFilter(tx, ttlByFamily, allowEmptyValues, type);
  }

  /**
   * Filter used to include cells visible to in-progress transactions on flush and commit.
   */
  static class IncludeInProgressFilter extends FilterBase {
    private final long visibilityUpperBound;
    private final Set<Long> invalidIds;
    private final Filter txFilter;

    public IncludeInProgressFilter(long upperBound, Collection<Long> invalids, Filter transactionFilter) {
      this.visibilityUpperBound = upperBound;
      this.invalidIds = Sets.newHashSet(invalids);
      this.txFilter = transactionFilter;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      // include all cells visible to in-progress transactions, except for those already marked as invalid
      long ts = cell.getTimestamp();
      if (ts > visibilityUpperBound) {
        // include everything that could still be in-progress except invalids
        if (invalidIds.contains(ts)) {
          return ReturnCode.SKIP;
        }
        return ReturnCode.INCLUDE;
      }
      return txFilter.filterKeyValue(cell);
    }
  }
}
