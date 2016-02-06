/*
 * Copyright © 2016 Cask Data, Inc.
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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * HBase 1.1 specific test for testing {@link CellSkipFilter}.
 */
public class CellSkipFilterTest {

  private static final String ROW1KEY = "row1";
  private static final String ROW2KEY = "row2";
  private static final String FAM1KEY = "fam1";
  private static final String COL1KEY = "col1";
  private static final String FAM2KEY = "fam2";
  private static final String COL2KEY = "col2";
  private static final String VALUE = "value";

  @Test
  public void testSkipFiltering() throws Exception {
    long timestamp = System.currentTimeMillis();
    // Test to check that we get NEXT_COL once the INCLUDE_AND_NEXT_COL is returned for the same key
    Filter filter = new CellSkipFilter(new MyFilter(0));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterKeyValue(newKeyValue(ROW1KEY, FAM1KEY, COL1KEY, VALUE,
                                                                              timestamp)));
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL, filter.filterKeyValue(newKeyValue(ROW1KEY, FAM1KEY, COL1KEY,
                                                                                           VALUE, timestamp - 1)));

    // Next call should get NEXT_COL instead of SKIP, as it would be returned by CellSkipFilter
    assertEquals(Filter.ReturnCode.NEXT_COL, filter.filterKeyValue(newKeyValue(ROW1KEY, FAM1KEY, COL1KEY, VALUE,
                                                                               timestamp - 2)));

    // Next call with the same key should return the NEXT_COL again, as it would be returned by CellSkipFilter
    assertEquals(Filter.ReturnCode.NEXT_COL, filter.filterKeyValue(newKeyValue(ROW1KEY, FAM1KEY, COL1KEY, VALUE,
                                                                               timestamp - 3)));

    // Since MyFilter counter is not incremented in the previous call, filtering for the different keyvalue should
    // give SKIP from MyFilter
    assertEquals(Filter.ReturnCode.SKIP, filter.filterKeyValue(newKeyValue(ROW1KEY, FAM2KEY, COL1KEY, VALUE,
                                                                           timestamp - 4)));

    // Test to check that we get NEXT_COL once the NEXT_COL is returned for the same key
    filter = new CellSkipFilter(new MyFilter(2));
    assertEquals(Filter.ReturnCode.SKIP, filter.filterKeyValue(newKeyValue(ROW1KEY, FAM1KEY, COL1KEY, VALUE,
                                                                           timestamp)));
    assertEquals(Filter.ReturnCode.NEXT_COL, filter.filterKeyValue(newKeyValue(ROW1KEY, FAM1KEY, COL1KEY, VALUE,
                                                                               timestamp - 1)));

    // Next call should get NEXT_COL instead of NEXT_ROW, as it would be returned by CellSkipFilter
    assertEquals(Filter.ReturnCode.NEXT_COL, filter.filterKeyValue(newKeyValue(ROW1KEY, FAM1KEY, COL1KEY, VALUE,
                                                                               timestamp - 2)));

    // Next call with the same key should return the NEXT_COL again, as it would be returned by CellSkipFilter
    assertEquals(Filter.ReturnCode.NEXT_COL, filter.filterKeyValue(newKeyValue(ROW1KEY, FAM1KEY, COL1KEY, VALUE,
                                                                               timestamp - 3)));

    // Since MyFilter counter is not incremented in the previous call, filtering for the different keyvalue should
    // give NEXT_ROW from MyFilter
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterKeyValue(newKeyValue(ROW1KEY, FAM1KEY, COL2KEY, VALUE,
                                                                               timestamp - 4)));

    // Next call with the new key should returned the SEEK_NEXT_USING_HINT
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterKeyValue(newKeyValue(ROW2KEY, FAM1KEY, COL1KEY,
                                                                                           VALUE, timestamp - 5)));
  }

  private KeyValue newKeyValue(String rowkey, String family, String column, String value, long timestamp) {
    return new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(family), Bytes.toBytes(column),
                        timestamp, Bytes.toBytes(value));
  }

  /**
   * Sample filter for testing. This filter maintains the {@link List} of {@link ReturnCode}s. It accepts the
   * start index in the list and start serving the return codes corresponding that that index. Every time the
   * return code is served, index is incremented.
   */
  class MyFilter extends FilterBase {

    private final List<ReturnCode> returnCodes;
    private int counter;

    public MyFilter(int startIndex) {
      returnCodes = Arrays.asList(ReturnCode.INCLUDE, ReturnCode.INCLUDE_AND_NEXT_COL, ReturnCode.SKIP,
                                  ReturnCode.NEXT_COL, ReturnCode.NEXT_ROW, ReturnCode.SEEK_NEXT_USING_HINT);
      counter = startIndex;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      ReturnCode code = returnCodes.get(counter % returnCodes.size());
      counter++;
      return code;
    }
  }
}
