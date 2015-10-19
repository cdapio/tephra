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

package co.cask.tephra;

import java.util.Arrays;

/**
 * Transaction details
 */
// NOTE: this class should have minimal dependencies as it is used in HBase CPs and other places where minimal classes
//       are available
public class Transaction {
  private final long readPointer;
  private final long txId;
  private final long writePointer;
  private final long[] invalids;
  private final long[] inProgress;
  private final long firstShortInProgress;
  private final TransactionType type;
  private final long[] checkpointWritePointers;

  private VisibilityLevel visibilityLevel = VisibilityLevel.SNAPSHOT;

  private static final long[] NO_EXCLUDES = { };
  public static final long NO_TX_IN_PROGRESS = Long.MAX_VALUE;

  public static final Transaction ALL_VISIBLE_LATEST =
    new Transaction(Long.MAX_VALUE, Long.MAX_VALUE, NO_EXCLUDES, NO_EXCLUDES, NO_TX_IN_PROGRESS, TransactionType.SHORT);

  /**
   * Defines the possible visibility levels for read operations.
   *
   * <p>
   * <ul>
   *   <li><code>SNAPSHOT</code> - uses the transaction's read snapshot, plus includes all write pointers from the
   *       current transaction</li>
   *   <li><code>SNAPSHOT_EXCLUDE_CURRENT</code> - uses the transaction's read snapshot, plus includes all write
   *       pointers from the current transaction, <strong>except</strong> the current write pointer
   *       (see {@link #getWritePointer()})</li>
   *   <li><code>SNAPSHOT_ALL</code> - uses the transaction's read snapshot, plus includes all write pointers from the
   *       current transaction. This visibility level will lead to raw fetch operations, where all versions
   *       (including deletes) visible to current transaction are returned.</li>
   * </ul>
   * </p>
   *
   * <p>The default value used is {@code SNAPSHOT}.</p>
   *
   * @see #setVisibility(VisibilityLevel)
   */
  public enum VisibilityLevel {
    SNAPSHOT,
    SNAPSHOT_EXCLUDE_CURRENT,
    SNAPSHOT_ALL
  }

  /**
   * Creates a new short transaction.
   * @param readPointer read pointer for transaction
   * @param txId unique identifier for the transaction
   * @param invalids list of invalid transactions to exclude while reading
   * @param inProgress list of in-progress transactions to exclude while reading
   * @param firstShortInProgress earliest in-progress short transaction
   */
  public Transaction(long readPointer, long txId, long[] invalids, long[] inProgress,
                     long firstShortInProgress) {
    this(readPointer, txId, invalids, inProgress, firstShortInProgress, TransactionType.SHORT);
  }

  /**
   * Creates a new transaction.
   * @param readPointer read pointer for transaction
   * @param txId unique identifier for the transaction
   * @param invalids list of invalid transactions to exclude while reading
   * @param inProgress list of in-progress transactions to exclude while reading
   * @param firstShortInProgress earliest in-progress short transaction
   * @param type transaction type
   */
  public Transaction(long readPointer, long txId, long[] invalids, long[] inProgress,
                     long firstShortInProgress, TransactionType type) {
    this(readPointer, txId, txId, invalids, inProgress, firstShortInProgress, type, new long[0],
        VisibilityLevel.SNAPSHOT);
  }

  /**
   * Creates a new transaction.
   * @param readPointer read pointer for transaction
   * @param txId unique identifier for the transaction
   * @param writePointer the current pointer to be used for any writes.
   *                      For new transactions, this will be the same as {@code txId}.  For checkpointed
   *                      transactions, this will be the most recent write pointer issued.
   * @param invalids list of invalid transactions to exclude while reading
   * @param inProgress list of in-progress transactions to exclude while reading
   * @param firstShortInProgress earliest in-progress short transaction
   * @param type transaction type
   * @param checkpointPointers the list of writer pointers added from checkpoints on the transaction
   * @param visibilityLevel the visibility level to use for transactional reads
   */
  public Transaction(long readPointer, long txId, long writePointer, long[] invalids, long[] inProgress,
                     long firstShortInProgress, TransactionType type, long[] checkpointPointers,
                     VisibilityLevel visibilityLevel) {
    this.readPointer = readPointer;
    this.txId = txId;
    this.writePointer = writePointer;
    this.invalids = invalids;
    this.inProgress = inProgress;
    this.firstShortInProgress = firstShortInProgress;
    this.type = type;
    this.checkpointWritePointers = checkpointPointers;
    this.visibilityLevel = visibilityLevel;
  }

  /**
   * Creates a new transaction for a checkpoint operation, copying all members from the original transaction,
   * with the updated checkpoint write pointers.
   *
   * @param toCopy the original transaction containing the state to copy
   * @param writePointer the new write pointer to use for the transaction
   * @param checkpointPointers the list of write pointers added from checkpoints on the transaction
   */
  public Transaction(Transaction toCopy, long writePointer, long[] checkpointPointers) {
    this(toCopy.getReadPointer(), toCopy.getTransactionId(), writePointer, toCopy.getInvalids(),
        toCopy.getInProgress(), toCopy.getFirstShortInProgress(), toCopy.getType(), checkpointPointers,
        toCopy.getVisibilityLevel());
  }

  public long getReadPointer() {
    return readPointer;
  }

  /**
   * Returns the initial write pointer assigned to the transaction.  This will remain the same for the life of the
   * transaction, and uniquely identifies it with the transaction service.  This value should be provided
   * to identify the transaction when calling any transaction lifecycle methods on the transaction service.
   */
  public long getTransactionId() {
    return txId;
  }

  /**
   * Returns the write pointer to be used in persisting any changes.  After a checkpoint is performed, this will differ
   * from {@link #getTransactionId()}.  This method should always be used when setting the timestamp for writes
   * in order to ensure that the correct value is used.
   */
  public long getWritePointer() {
    return writePointer;
  }

  public long[] getInvalids() {
    return invalids;
  }

  public long[] getInProgress() {
    return inProgress;
  }

  public long getFirstInProgress() {
    return inProgress.length == 0 ? NO_TX_IN_PROGRESS : inProgress[0];
  }

  public TransactionType getType() {
    return type;
  }

  /**
   * @return transaction id {@code X} such that any of the transactions newer than {@code X} may be invisible to this<p>
   *         NOTE: the returned tx id can be invalid.
   */
  public long getVisibilityUpperBound() {
    // NOTE: in some cases when we do not provide visibility guarantee, we set readPointer to MAX value, but
    //       at same time we don't want that to case cleanup everything as this is used for tx janitor + ttl to see
    //       what can be cleaned up. When non-tx mode is implemented better, we should not need this check
    return inProgress.length == 0 ? Math.min(txId - 1, readPointer) : inProgress[0] - 1;
  }

  public long getFirstShortInProgress() {
    return firstShortInProgress;
  }

  /**
   * Returns true if the given version corresponds to a transaction that was in-progress at the time this transaction
   * started.
   */
  public boolean isInProgress(long version) {
    return Arrays.binarySearch(inProgress, version) >= 0;
  }

  /**
   * Returns true if the given version is present in one of the arrays of excluded versions (in-progress and
   * invalid transactions).
   */
  public boolean isExcluded(long version) {
    return Arrays.binarySearch(inProgress, version) >= 0
      || Arrays.binarySearch(invalids, version) >= 0;
  }

  /**
   * Returns true if the the given version corresponds to one of the checkpoint versions in the current
   * transaction.
   */
  public boolean isCheckpoint(long version) {
    return Arrays.binarySearch(checkpointWritePointers, version) >= 0;
  }

  /**
   * Returns whether or not the given version should be visible to the current transaction.  A version will be visible
   * if it was successfully committed prior to the current transaction starting, or was written by the current
   * transaction (using either the current write pointer or the write pointer from a prior checkpoint).
   *
   * @param version the data version to check for visibility
   * @return true if the version is visible, false if it should be hidden (filtered)
   *
   * @see #setVisibility(VisibilityLevel) to control whether the current write pointer is visible.
   */
  public boolean isVisible(long version) {
    // either it was committed before or the change belongs to current tx
    return (version <= getReadPointer() && !isExcluded(version)) ||
        (isCurrentWrite(version) &&
            (visibilityLevel != VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT || writePointer != version));
  }

  /**
   * Returns whether the given version was written by the current transaction.
   *
   * @param version the data version
   * @return true if version was written by current transaction, false otherwise.
   */
  public boolean isCurrentWrite(long version) {
    return writePointer == version || txId == version || isCheckpoint(version);
  }

  /**
   * Sets the visibility level for read operations.
   */
  public void setVisibility(VisibilityLevel level) {
    this.visibilityLevel = level;
  }

  /**
   * Returns the currently set visibility level.
   */
  public VisibilityLevel getVisibilityLevel() {
    return visibilityLevel;
  }

  public boolean hasExcludes() {
    return invalids.length > 0 || inProgress.length > 0;
  }


  public int excludesSize() {
    return invalids.length + inProgress.length;
  }

  /**
   * Returns any prior write pointers used in the current transaction.  A new write pointer is issued when the
   * {@code TransactionContext.checkpoint(Transaction)} operation is called, and the prior write pointer is added
   * to the array of checkpoint write pointers.
`   */
  public long[] getCheckpointWritePointers() {
    return checkpointWritePointers;
  }

  @Override
  public String toString() {
    return new StringBuilder(100)
      .append(Transaction.class.getSimpleName())
      .append('{')
      .append("readPointer: ").append(readPointer)
      .append(", transactionId: ").append(txId)
      .append(", writePointer: ").append(writePointer)
      .append(", invalids: ").append(Arrays.toString(invalids))
      .append(", inProgress: ").append(Arrays.toString(inProgress))
      .append(", firstShortInProgress: ").append(firstShortInProgress)
      .append(", type: ").append(type)
      .append(", checkpointWritePointers: ").append(Arrays.toString(checkpointWritePointers))
      .append(", visibilityLevel: ").append(visibilityLevel)
      .append('}')
      .toString();
  }
}
