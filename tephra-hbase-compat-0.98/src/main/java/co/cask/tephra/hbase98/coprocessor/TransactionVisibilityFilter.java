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

package co.cask.tephra.hbase98.coprocessor;

import co.cask.tephra.Transaction;
import co.cask.tephra.TxConstants;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Applies filtering of data based on transactional visibility (HBase 0.98+ specific version).
 * Note: this is intended for server-side use only, as additional properties need to be set on
 * any {@code Scan} or {@code Get} operation performed.
 */
public class TransactionVisibilityFilter extends FilterBase {
  private final Transaction tx;
  // oldest visible timestamp by column family, used to apply TTL when reading
  private final Map<byte[], Long> oldestTsByFamily;
  // if false, empty values will be interpreted as deletes
  private final boolean allowEmptyValues;
  // whether or not we can remove delete markers
  // these can only be safely removed when we are traversing all storefiles
  private final boolean clearDeletes;
  // optional sub-filter to apply to visible cells
  private final Filter cellFilter;

  // since we traverse KVs in order, cache the current oldest TS to avoid map lookups per KV
  private byte[] currentFamily = new byte[0];
  private long currentOldestTs;

  private DeleteTracker deleteTracker = new DeleteTracker();

  /**
   * Creates a new {@link org.apache.hadoop.hbase.filter.Filter} for returning data only from visible transactions.
   *
   * @param tx the current transaction to apply.  Only data visible to this transaction will be returned.
   * @param ttlByFamily map of time-to-live (TTL) (in milliseconds) by column family name
   * @param allowEmptyValues if {@code true} cells with empty {@code byte[]} values will be returned, if {@code false}
   *                         these will be interpreted as "delete" markers and the column will be filtered out
   * @param scanType the type of scan operation being performed
   */
  public TransactionVisibilityFilter(Transaction tx, Map<byte[], Long> ttlByFamily, boolean allowEmptyValues,
                                     ScanType scanType) {
    this(tx, ttlByFamily, allowEmptyValues, scanType, null);
  }

  /**
   * Creates a new {@link org.apache.hadoop.hbase.filter.Filter} for returning data only from visible transactions.
   *
   * @param tx the current transaction to apply.  Only data visible to this transaction will be returned.
   * @param ttlByFamily map of time-to-live (TTL) (in milliseconds) by column family name
   * @param allowEmptyValues if {@code true} cells with empty {@code byte[]} values will be returned, if {@code false}
   *                         these will be interpreted as "delete" markers and the column will be filtered out
   * @param scanType the type of scan operation being performed
   * @param cellFilter if non-null, this filter will be applied to all cells visible to the current transaction, by
   *                   calling {@link Filter#filterKeyValue(org.apache.hadoop.hbase.Cell)}.  If null, then
   *                   {@link Filter.ReturnCode#INCLUDE_AND_NEXT_COL} will be returned instead.
   */
  public TransactionVisibilityFilter(Transaction tx, Map<byte[], Long> ttlByFamily, boolean allowEmptyValues,
                                     ScanType scanType, @Nullable Filter cellFilter) {
    this.tx = tx;
    this.oldestTsByFamily = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], Long> ttlEntry : ttlByFamily.entrySet()) {
      long familyTTL = ttlEntry.getValue();
      oldestTsByFamily.put(ttlEntry.getKey(),
                           familyTTL <= 0 ? 0 : tx.getVisibilityUpperBound() - familyTTL * TxConstants.MAX_TX_PER_MS);
    }
    this.allowEmptyValues = allowEmptyValues;
    this.clearDeletes =
        scanType == ScanType.COMPACT_DROP_DELETES || scanType == ScanType.USER_SCAN;
    this.cellFilter = cellFilter;
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    if (!CellUtil.matchingFamily(cell, currentFamily)) {
      // column family changed
      currentFamily = CellUtil.cloneFamily(cell);
      Long familyOldestTs = oldestTsByFamily.get(currentFamily);
      currentOldestTs = familyOldestTs != null ? familyOldestTs : 0;
      deleteTracker.reset();
    }
    // need to apply TTL for the column family here
    long kvTimestamp = cell.getTimestamp();
    if (kvTimestamp < currentOldestTs) {
      // passed TTL for this column, seek to next
      return ReturnCode.NEXT_COL;
    } else if (tx.isVisible(kvTimestamp)) {
      if (deleteTracker.isFamilyDelete(cell)) {
        deleteTracker.addFamilyDelete(cell);
        if (clearDeletes) {
          return ReturnCode.NEXT_COL;
        } else {
          return ReturnCode.INCLUDE_AND_NEXT_COL;
        }
      }
      // check if masked by family delete
      if (deleteTracker.isDeleted(cell)) {
        return ReturnCode.NEXT_COL;
      }
      // check for column delete
      if (cell.getValueLength() == 0 && !allowEmptyValues) {
        if (clearDeletes) {
          // skip "deleted" cell
          return ReturnCode.NEXT_COL;
        } else {
          // keep the marker but skip any remaining versions
          return ReturnCode.INCLUDE_AND_NEXT_COL;
        }
      }
      // cell is visible
      if (cellFilter != null) {
        return cellFilter.filterKeyValue(cell);
      } else {
        // as soon as we find a KV to include we can move to the next column
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      }
    } else {
      return ReturnCode.SKIP;
    }
  }

  @Override
  public void reset() {
    deleteTracker.reset();
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return super.toByteArray();
  }

  private static final class DeleteTracker {
    private long familyDeleteTs;

    public boolean isFamilyDelete(Cell cell) {
      return CellUtil.matchingQualifier(cell, TxConstants.FAMILY_DELETE_QUALIFIER) &&
              CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY);
    }

    public void addFamilyDelete(Cell delete) {
      this.familyDeleteTs = delete.getTimestamp();
    }

    public boolean isDeleted(Cell cell) {
      return cell.getTimestamp() < familyDeleteTs;
    }

    public void reset() {
      this.familyDeleteTs = 0;
    }
  }
}
