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

package co.cask.tephra.hbase94.coprocessor;

import co.cask.tephra.Transaction;
import co.cask.tephra.TxConstants;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Applies filtering of data based on transactional visibility (HBase 0.94 specific version).
 * Note: this is intended for server-side use only, as additional properties need to be set on
 * any {@code Scan} or {@code Get} operation performed.
 */
public class TransactionVisibilityFilter extends FilterBase {
  private final Transaction tx;
  // oldest visible timestamp by column family, used to apply TTL when reading
  private final Map<byte[], Long> oldestTsByFamily;
  // if false, empty values will be interpreted as deletes
  private final boolean allowEmptyValues;
  // optional sub-filter to apply to visible cells
  private final Filter cellFilter;

  // since we traverse KVs in order, cache the current oldest TS to avoid map lookups per KV
  private byte[] currentFamily = new byte[0];
  private long currentOldestTs;

  /**
   * Creates a new {@link org.apache.hadoop.hbase.filter.Filter} for returning data only from visible transactions.
   *
   * @param tx The current transaction to apply.  Only data visible to this transaction will be returned.
   * @param ttlByFamily Map of time-to-live (TTL) (in milliseconds) by column family name.
   * @param allowEmptyValues If {@code true} cells with empty {@code byte[]} values will be returned, if {@code false}
   *                         these will be interpreted as "delete" markers and the column will be filtered out.
   */
  public TransactionVisibilityFilter(Transaction tx, Map<byte[], Long> ttlByFamily, boolean allowEmptyValues) {
    this(tx, ttlByFamily, allowEmptyValues, null);
  }

  /**
   * Creates a new {@link org.apache.hadoop.hbase.filter.Filter} for returning data only from visible transactions.
   *
   * @param tx The current transaction to apply.  Only data visible to this transaction will be returned.
   * @param ttlByFamily Map of time-to-live (TTL) (in milliseconds) by column family name.
   * @param allowEmptyValues If {@code true} cells with empty {@code byte[]} values will be returned, if {@code false}
   *                         these will be interpreted as "delete" markers and the column will be filtered out.
   * @param cellFilter If non-null, this filter will be applied to all cells visible to the current transaction, by
   *                   calling {@link Filter#filterKeyValue(org.apache.hadoop.hbase.KeyValue)}.  If null, then
   *                   {@link Filter.ReturnCode#INCLUDE_AND_NEXT_COL} will be returned instead.
   */
  public TransactionVisibilityFilter(Transaction tx, Map<byte[], Long> ttlByFamily, boolean allowEmptyValues,
                                     @Nullable Filter cellFilter) {
    this.tx = tx;
    this.oldestTsByFamily = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], Long> ttlEntry : ttlByFamily.entrySet()) {
      long familyTTL = ttlEntry.getValue();
      oldestTsByFamily.put(ttlEntry.getKey(),
                           familyTTL <= 0 ? 0 : tx.getVisibilityUpperBound() - familyTTL * TxConstants.MAX_TX_PER_MS);
    }
    this.allowEmptyValues = allowEmptyValues;
    this.cellFilter = cellFilter;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    if (!Bytes.equals(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
                      currentFamily, 0, currentFamily.length)) {
      // column family changed
      currentFamily = kv.getFamily();
      Long familyOldestTs = oldestTsByFamily.get(currentFamily);
      currentOldestTs = familyOldestTs != null ? familyOldestTs : 0;
    }
    // need to apply TTL for the column family here
    long kvTimestamp = kv.getTimestamp();
    if (kvTimestamp < currentOldestTs) {
      // passed TTL for this column, seek to next
      return ReturnCode.NEXT_COL;
    } else if (tx.isVisible(kvTimestamp)) {
      if (kv.getValueLength() == 0 && !allowEmptyValues) {
        // skip "deleted" cell
        return ReturnCode.NEXT_COL;
      }
      // cell is visible
      if (cellFilter != null) {
        return cellFilter.filterKeyValue(kv);
      } else {
        // as soon as we find a KV to include we can move to the next column
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      }
    } else {
      return ReturnCode.SKIP;
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException("Filter does not support serialization!");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("Filter does not support serialization!");
  }
}
