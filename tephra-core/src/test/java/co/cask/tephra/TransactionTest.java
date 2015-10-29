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

package co.cask.tephra;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

/**
 *
 */
public class TransactionTest {
  // Current transaction id
  private final long txId = 200L;
  // Read pointer for current transaction
  private final long readPointer = 100L;
  // Transactions committed before current transaction was started.
  private final Set<Long> priorCommitted = ImmutableSet.of(80L, 99L, 100L);
  // Transactions committed after current transaction was started.
  private final Set<Long> postCommitted = ImmutableSet.of(150L, 180L, 210L);
  // Invalid transactions before current transaction was started.
  private final Set<Long> priorInvalids = ImmutableSet.of(90L, 110L, 190L);
  // Invalid transactions after current transaction was started.
  private final Set<Long> postInvalids = ImmutableSet.of(201L, 221L, 231L);
  // Transactions in progress before current transaction was started.
  private final Set<Long> priorInProgress = ImmutableSet.of(95L, 120L, 150L);
  // Transactions in progress after current transaction was started.
  private final Set<Long> postInProgress = ImmutableSet.of(205L, 215L, 300L);

  @Test
  public void testSnapshotVisibility() throws Exception {
    Transaction.VisibilityLevel visibilityLevel = Transaction.VisibilityLevel.SNAPSHOT;

    Set<Long> checkPointers = ImmutableSet.of(220L, 250L);
    Transaction tx = new Transaction(readPointer, txId, 250L, toSortedArray(priorInvalids),
                                     toSortedArray(priorInProgress), 95L,
                                     TransactionType.SHORT, toSortedArray(checkPointers),
                                     visibilityLevel);
    Set<Long> visibleCurrent = ImmutableSet.of(200L, 220L, 250L);
    Set<Long> notVisibleCurrent = ImmutableSet.of();
    assertVisibility(priorCommitted, postCommitted, priorInvalids, postInvalids, priorInProgress, postInProgress,
                     visibleCurrent, notVisibleCurrent, tx);

    checkPointers = ImmutableSet.of();
    tx = new Transaction(readPointer, txId, txId, toSortedArray(priorInvalids), toSortedArray(priorInProgress), 95L,
                                     TransactionType.SHORT, toSortedArray(checkPointers),
                         visibilityLevel);
    visibleCurrent = ImmutableSet.of(txId);
    notVisibleCurrent = ImmutableSet.of();
    assertVisibility(priorCommitted, postCommitted, priorInvalids, postInvalids, priorInProgress, postInProgress,
                     visibleCurrent, notVisibleCurrent, tx);
  }

  @Test
  public void testSnapshotExcludeVisibility() throws Exception {
    Transaction.VisibilityLevel visibilityLevel = Transaction.VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT;

    Set<Long> checkPointers = ImmutableSet.of(220L, 250L);
    Transaction tx = new Transaction(readPointer, txId, 250L, toSortedArray(priorInvalids),
                                     toSortedArray(priorInProgress), 95L,
                                     TransactionType.SHORT, toSortedArray(checkPointers),
                                     visibilityLevel);
    Set<Long> visibleCurrent = ImmutableSet.of(200L, 220L);
    Set<Long> notVisibleCurrent = ImmutableSet.of(250L);
    assertVisibility(priorCommitted, postCommitted, priorInvalids, postInvalids, priorInProgress, postInProgress,
                     visibleCurrent, notVisibleCurrent, tx);

    checkPointers = ImmutableSet.of();
    tx = new Transaction(readPointer, txId, txId, toSortedArray(priorInvalids), toSortedArray(priorInProgress), 95L,
                         TransactionType.SHORT, toSortedArray(checkPointers),
                         visibilityLevel);
    visibleCurrent = ImmutableSet.of();
    notVisibleCurrent = ImmutableSet.of(txId);
    assertVisibility(priorCommitted, postCommitted, priorInvalids, postInvalids, priorInProgress, postInProgress,
                     visibleCurrent, notVisibleCurrent, tx);
  }

  @Test
  public void testSnapshotAllVisibility() throws Exception {
    Transaction.VisibilityLevel visibilityLevel = Transaction.VisibilityLevel.SNAPSHOT_ALL;

    Set<Long> checkPointers = ImmutableSet.of(220L, 250L);
    Transaction tx = new Transaction(readPointer, txId, 250L, toSortedArray(priorInvalids),
                                     toSortedArray(priorInProgress), 95L,
                                     TransactionType.SHORT, toSortedArray(checkPointers),
                                     visibilityLevel);
    Set<Long> visibleCurrent = ImmutableSet.of(200L, 220L, 250L);
    Set<Long> notVisibleCurrent = ImmutableSet.of();
    assertVisibility(priorCommitted, postCommitted, priorInvalids, postInvalids, priorInProgress, postInProgress,
                     visibleCurrent, notVisibleCurrent, tx);

    checkPointers = ImmutableSet.of();
    tx = new Transaction(readPointer, txId, txId, toSortedArray(priorInvalids),
                         toSortedArray(priorInProgress), 95L,
                         TransactionType.SHORT, toSortedArray(checkPointers),
                         visibilityLevel);
    visibleCurrent = ImmutableSet.of(txId);
    notVisibleCurrent = ImmutableSet.of();
    assertVisibility(priorCommitted, postCommitted, priorInvalids, postInvalids, priorInProgress, postInProgress,
                     visibleCurrent, notVisibleCurrent, tx);
  }

  private void assertVisibility(Set<Long> priorCommitted, Set<Long> postCommitted, Set<Long> priorInvalids,
                                Set<Long> postInvalids, Set<Long> priorInProgress, Set<Long> postInProgress,
                                Set<Long> visibleCurrent, Set<Long> notVisibleCurrent,
                                Transaction tx) {
    // Verify visible snapshots of tx are visible
    for (long t : visibleCurrent) {
      Assert.assertTrue("Assertion error for version = " + t, tx.isVisible(t));
    }

    // Verify not visible snapshots of tx are not visible
    for (long t : notVisibleCurrent) {
      Assert.assertFalse("Assertion error for version = " + t, tx.isVisible(t));
    }

    // Verify prior committed versions are visible
    for (long t : priorCommitted) {
      Assert.assertTrue("Assertion error for version = " + t, tx.isVisible(t));
    }

    // Verify versions committed after tx started, and not part of tx are not visible
    for (long t : postCommitted) {
      Assert.assertFalse("Assertion error for version = " + t, tx.isVisible(t));
    }

    // Verify invalid and in-progress versions are not visible
    for (long t : Iterables.concat(priorInvalids, postInvalids, priorInProgress, postInProgress)) {
      Assert.assertFalse("Assertion error for version = " + t, tx.isVisible(t));
    }
  }

  private long[] toSortedArray(Set<Long> set) {
    long[] array = Longs.toArray(set);
    Arrays.sort(array);
    return array;
  }
}
