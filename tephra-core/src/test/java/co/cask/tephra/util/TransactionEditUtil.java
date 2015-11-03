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

package co.cask.tephra.util;

import co.cask.tephra.ChangeId;
import co.cask.tephra.TransactionType;
import co.cask.tephra.persist.TransactionEdit;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Util class for {@link TransactionEdit} related tests.
 */
public final class TransactionEditUtil {
  private static Random random = new Random();

  /**
   * Generates a number of semi-random {@link TransactionEdit} instances.
   * These are just randomly selected from the possible states, so would not necessarily reflect a real-world
   * distribution.
   *
   * @param numEntries how many entries to generate in the returned list.
   * @return a list of randomly generated transaction log edits.
   */
  public static List<TransactionEdit> createRandomEdits(int numEntries) {
    List<TransactionEdit> edits = Lists.newArrayListWithCapacity(numEntries);
    for (int i = 0; i < numEntries; i++) {
      TransactionEdit.State nextType = TransactionEdit.State.values()[random.nextInt(6)];
      long writePointer = Math.abs(random.nextLong());
      switch (nextType) {
        case INPROGRESS:
          edits.add(
            TransactionEdit.createStarted(writePointer, writePointer - 1,
                                          System.currentTimeMillis() + 300000L, TransactionType.SHORT));
          break;
        case COMMITTING:
          edits.add(TransactionEdit.createCommitting(writePointer, generateChangeSet(10)));
          break;
        case COMMITTED:
          edits.add(TransactionEdit.createCommitted(writePointer, generateChangeSet(10), writePointer + 1,
                                                    random.nextBoolean()));
          break;
        case INVALID:
          edits.add(TransactionEdit.createInvalid(writePointer));
          break;
        case ABORTED:
          edits.add(TransactionEdit.createAborted(writePointer, TransactionType.SHORT, null));
          break;
        case MOVE_WATERMARK:
          edits.add(TransactionEdit.createMoveWatermark(writePointer));
          break;
      }
    }
    return edits;
  }

  private static Set<ChangeId> generateChangeSet(int numEntries) {
    Set<ChangeId> changes = Sets.newHashSet();
    for (int i = 0; i < numEntries; i++) {
      byte[] bytes = new byte[8];
      random.nextBytes(bytes);
      changes.add(new ChangeId(bytes));
    }
    return changes;
  }
}
