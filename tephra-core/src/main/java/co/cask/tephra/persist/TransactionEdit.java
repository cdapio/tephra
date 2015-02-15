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

package co.cask.tephra.persist;

import co.cask.tephra.ChangeId;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionType;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * Represents a transaction state change in the {@link TransactionLog}.
 */
public class TransactionEdit implements Writable {
  // provides serde for current version
  private static final TransactionEditCodec CODEC_V3 = new TransactionEditCodecV3();
  private static final byte V3 = -3;
  // provides serde for old but still supported version, should not be used for writing
  private static final TransactionEditCodec CODEC_V2 = new TransactionEditCodecV2();
  private static final byte V2 = -2;
  private static final TransactionEditCodec CODEC_V1 = new TransactionEditCodecV1();
  private static final byte V1 = -1;

  /**
   * The possible state changes for a transaction.
   */
  public enum State {
    INPROGRESS, COMMITTING, COMMITTED, INVALID, ABORTED, MOVE_WATERMARK
  }

  private long writePointer;

  /**
   * stores the value of visibility upper bound
   * (see {@link TransactionManager.InProgressTx#getVisibilityUpperBound()})
   * for edit of {@link State#INPROGRESS} only
   */
  private long visibilityUpperBound;
  private long commitPointer;
  private long expirationDate;
  private State state;
  private Set<ChangeId> changes = Sets.newHashSet();
  /** Whether or not the COMMITTED change should be fully committed. */
  private boolean canCommit;
  private TransactionType type;

  // for Writable
  public TransactionEdit() {
  }

  // package private for testing
  TransactionEdit(long writePointer, long visibilityUpperBound, State state, long expirationDate,
                          Set<ChangeId> changes, long commitPointer, boolean canCommit, TransactionType type) {
    this.writePointer = writePointer;
    this.visibilityUpperBound = visibilityUpperBound;
    this.state = state;
    this.expirationDate = expirationDate;
    if (changes != null) {
      this.changes = changes;
    }
    this.commitPointer = commitPointer;
    this.canCommit = canCommit;
    this.type = type;
  }

  /**
   * Returns the transaction write pointer assigned for the state change.
   */
  public long getWritePointer() {
    return writePointer;
  }

  public long getVisibilityUpperBound() {
    return visibilityUpperBound;
  }

  /**
   * Returns the type of state change represented.
   */
  public State getState() {
    return state;
  }

  /**
   * Returns any expiration timestamp (in milliseconds) associated with the state change.  This should only
   * be populated for changes of type {@link State#INPROGRESS}.
   */
  public long getExpiration() {
    return expirationDate;
  }

  /**
   * @return the set of changed row keys associated with the state change.  This is only populated for edits
   * of type {@link State#COMMITTING} or {@link State#COMMITTED}.
   */
  public Set<ChangeId> getChanges() {
    return changes;
  }

  /**
   * Returns the write pointer used to commit the row key change set.  This is only populated for edits of type
   * {@link State#COMMITTED}.
   */
  public long getCommitPointer() {
    return commitPointer;
  }

  /**
   * Returns whether or not the transaction should be moved to the committed set.  This is only populated for edits
   * of type {@link State#COMMITTED}.
   */
  public boolean getCanCommit() {
    return canCommit;
  }

  /**
   * Returns the transaction type. This is only populated for edits of type {@link State#INPROGRESS} or 
   * {@link State#ABORTED}.
   */
  public TransactionType getType() {
    return type;
  }

  /**
   * Creates a new instance in the {@link State#INPROGRESS} state.
   */
  public static TransactionEdit createStarted(long writePointer, long visibilityUpperBound,
                                              long expirationDate, TransactionType type) {
    return new TransactionEdit(writePointer, visibilityUpperBound, State.INPROGRESS,
                               expirationDate, null, 0L, false, type);
  }

  /**
   * Creates a new instance in the {@link State#COMMITTING} state.
   */
  public static TransactionEdit createCommitting(long writePointer, Set<ChangeId> changes) {
    return new TransactionEdit(writePointer, 0L, State.COMMITTING, 0L, changes, 0L, false, null);
  }

  /**
   * Creates a new instance in the {@link State#COMMITTED} state.
   */
  public static TransactionEdit createCommitted(long writePointer, Set<ChangeId> changes, long nextWritePointer,
                                                boolean canCommit) {
    return new TransactionEdit(writePointer, 0L, State.COMMITTED, 0L, changes, nextWritePointer, canCommit, null);
  }

  /**
   * Creates a new instance in the {@link State#ABORTED} state.
   */
  public static TransactionEdit createAborted(long writePointer, TransactionType type) {
    return new TransactionEdit(writePointer, 0L, State.ABORTED, 0L, null, 0L, false, type);
  }

  /**
   * Creates a new instance in the {@link State#INVALID} state.
   */
  public static TransactionEdit createInvalid(long writePointer) {
    return new TransactionEdit(writePointer, 0L, State.INVALID, 0L, null, 0L, false, null);
  }

  /**
   * Creates a new instance in the {@link State#MOVE_WATERMARK} state.
   */
  public static TransactionEdit createMoveWatermark(long writePointer) {
    return new TransactionEdit(writePointer, 0L, State.MOVE_WATERMARK, 0L, null, 0L, false, null);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    CODEC_V3.encode(this, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();
    switch (version) {
      case V3:
        CODEC_V3.decode(this, in);
        break;
      case V2:
        CODEC_V2.decode(this, in);
        break;
      case V1:
        CODEC_V1.decode(this, in);
        break;
      default:
        throw new IOException("Unexpected version for edit: " + version);
    }
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TransactionEdit)) {
      return false;
    }

    TransactionEdit that = (TransactionEdit) o;

    return Objects.equal(this.writePointer, that.writePointer) &&
      Objects.equal(this.visibilityUpperBound, that.visibilityUpperBound) &&
      Objects.equal(this.commitPointer, that.commitPointer) &&
      Objects.equal(this.expirationDate, that.expirationDate) &&
      Objects.equal(this.state, that.state) &&
      Objects.equal(this.changes, that.changes) &&
      Objects.equal(this.canCommit, that.canCommit) &&
      Objects.equal(this.type, that.type);
  }
  
  @Override
  public final int hashCode() {
    return Objects.hashCode(writePointer, visibilityUpperBound, commitPointer, expirationDate, state, changes,
                            canCommit, type);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("writePointer", writePointer)
      .add("visibilityUpperBound", visibilityUpperBound)
      .add("commitPointer", commitPointer)
      .add("expiration", expirationDate)
      .add("state", state)
      .add("changesSize", changes != null ? changes.size() : 0)
      .add("canCommit", canCommit)
      .add("type", type)
      .toString();
  }

  // package-private for unit-test access
  static interface TransactionEditCodec {
    // doesn't read version field
    void decode(TransactionEdit dest, DataInput in) throws IOException;

    // writes version field
    void encode(TransactionEdit src, DataOutput out) throws IOException;
  }

  // package-private for unit-test access
  static class TransactionEditCodecV1 implements TransactionEditCodec {
    @Override
    public void decode(TransactionEdit src, DataInput in) throws IOException {
      if (src.changes == null) {
        src.changes = Sets.newHashSet();
      } else {
        src.changes.clear();
      }

      src.writePointer = in.readLong();
      // 1st version did not store this info. It is safe to set firstInProgress to 0, it may decrease performance until
      // this tx is finished, but correctness will be preserved.
      src.visibilityUpperBound = 0;
      int stateIdx = in.readInt();
      try {
        src.state = TransactionEdit.State.values()[stateIdx];
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IOException("State enum ordinal value is out of range: " + stateIdx);
      }
      src.expirationDate = in.readLong();
      src.commitPointer = in.readLong();
      src.canCommit = in.readBoolean();
      int changeSize = in.readInt();
      for (int i = 0; i < changeSize; i++) {
        int currentLength = in.readInt();
        byte[] currentBytes = new byte[currentLength];
        in.readFully(currentBytes);
        src.changes.add(new ChangeId(currentBytes));
      }
    }

    /** @deprecated use {@link TransactionEditCodecV3} instead, it is still here for unit-tests only */
    @Override
    @Deprecated
    public void encode(TransactionEdit src, DataOutput out) throws IOException {
      out.writeByte(V1);
      out.writeLong(src.writePointer);
      // use ordinal for predictable size, though this does not support evolution
      out.writeInt(src.state.ordinal());
      out.writeLong(src.expirationDate);
      out.writeLong(src.commitPointer);
      out.writeBoolean(src.canCommit);
      if (src.changes == null) {
        out.writeInt(0);
      } else {
        out.writeInt(src.changes.size());
        for (ChangeId c : src.changes) {
          byte[] cKey = c.getKey();
          out.writeInt(cKey.length);
          out.write(cKey);
        }
      }
      // NOTE: we didn't have visibilityUpperBound in V1, it was added in V2
      // we didn't have transaction type in V1 and V2, it was added in V3
    }
  }

  // package-private for unit-test access
  static class TransactionEditCodecV2 implements TransactionEditCodec {
    @Override
    public void decode(TransactionEdit dest, DataInput in) throws IOException {
      if (dest.changes == null) {
        dest.changes = Sets.newHashSet();
      } else {
        dest.changes.clear();
      }

      dest.writePointer = in.readLong();
      int stateIdx = in.readInt();
      try {
        dest.state = TransactionEdit.State.values()[stateIdx];
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IOException("State enum ordinal value is out of range: " + stateIdx);
      }
      dest.expirationDate = in.readLong();
      dest.commitPointer = in.readLong();
      dest.canCommit = in.readBoolean();
      int changeSize = in.readInt();
      for (int i = 0; i < changeSize; i++) {
        int currentLength = in.readInt();
        byte[] currentBytes = new byte[currentLength];
        in.readFully(currentBytes);
        dest.changes.add(new ChangeId(currentBytes));
      }
      dest.visibilityUpperBound = in.readLong();
    }

    /** @deprecated use {@link TransactionEditCodecV3} instead, it is still here for unit-tests only */
    @Override
    public void encode(TransactionEdit src, DataOutput out) throws IOException {
      out.writeByte(V2);
      out.writeLong(src.writePointer);
      // use ordinal for predictable size, though this does not support evolution
      out.writeInt(src.state.ordinal());
      out.writeLong(src.expirationDate);
      out.writeLong(src.commitPointer);
      out.writeBoolean(src.canCommit);
      if (src.changes == null) {
        out.writeInt(0);
      } else {
        out.writeInt(src.changes.size());
        for (ChangeId c : src.changes) {
          byte[] cKey = c.getKey();
          out.writeInt(cKey.length);
          out.write(cKey);
        }
      }
      out.writeLong(src.visibilityUpperBound);
      // NOTE: we didn't have transaction type in V1 and V2, it was added in V3
    }
  }
  
  // TODO: refactor to avoid duplicate code among different version of codecs
  // package-private for unit-test access
  static class TransactionEditCodecV3 implements TransactionEditCodec {
    @Override
    public void decode(TransactionEdit dest, DataInput in) throws IOException {
      if (dest.changes == null) {
        dest.changes = Sets.newHashSet();
      } else {
        dest.changes.clear();
      }

      dest.writePointer = in.readLong();
      int stateIdx = in.readInt();
      try {
        dest.state = TransactionEdit.State.values()[stateIdx];
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IOException("State enum ordinal value is out of range: " + stateIdx);
      }
      dest.expirationDate = in.readLong();
      dest.commitPointer = in.readLong();
      dest.canCommit = in.readBoolean();
      int changeSize = in.readInt();
      for (int i = 0; i < changeSize; i++) {
        int currentLength = in.readInt();
        byte[] currentBytes = new byte[currentLength];
        in.readFully(currentBytes);
        dest.changes.add(new ChangeId(currentBytes));
      }
      dest.visibilityUpperBound = in.readLong();
      int typeIdx = in.readInt();
      // null transaction type is represented as -1
      if (typeIdx < 0) {
        dest.type = null;
      } else {
        try {
          dest.type = TransactionType.values()[typeIdx];
        } catch (ArrayIndexOutOfBoundsException e) {
          throw new IOException("Type enum ordinal value is out of range: " + typeIdx);
        }
      }
    }

    @Override
    public void encode(TransactionEdit src, DataOutput out) throws IOException {
      out.writeByte(V3);
      out.writeLong(src.writePointer);
      // use ordinal for predictable size, though this does not support evolution
      out.writeInt(src.state.ordinal());
      out.writeLong(src.expirationDate);
      out.writeLong(src.commitPointer);
      out.writeBoolean(src.canCommit);
      if (src.changes == null) {
        out.writeInt(0);
      } else {
        out.writeInt(src.changes.size());
        for (ChangeId c : src.changes) {
          byte[] cKey = c.getKey();
          out.writeInt(cKey.length);
          out.write(cKey);
        }
      }
      out.writeLong(src.visibilityUpperBound);
      // null transaction type is represented as -1
      if (src.type == null) {
        out.writeInt(-1);
      } else {
        out.writeInt(src.type.ordinal());
      }
    }
  }
}
