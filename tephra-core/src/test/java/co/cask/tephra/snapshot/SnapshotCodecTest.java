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

package co.cask.tephra.snapshot;

import co.cask.tephra.ChangeId;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionType;
import co.cask.tephra.TxConstants;
import co.cask.tephra.persist.TransactionSnapshot;
import co.cask.tephra.persist.TransactionStateStorage;
import co.cask.tephra.runtime.ConfigModule;
import co.cask.tephra.runtime.DiscoveryModules;
import co.cask.tephra.runtime.TransactionModules;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to {@link SnapshotCodec} implementations.
 */
public class SnapshotCodecTest {
  @ClassRule
  public static TemporaryFolder tmpDir = new TemporaryFolder();

  /**
   * In-progress LONG transactions written with DefaultSnapshotCodec will not have the type serialized as part of
   * the data.  Since these transactions also contain a non-negative expiration, we need to ensure we reset the type
   * correctly when the snapshot is loaded.
   */
  @Test
  public void testDefaultToV3Compatibility() throws Exception {
    long now = System.currentTimeMillis();
    long nowWritePointer = now * TxConstants.MAX_TX_PER_MS;
    /*
     * Snapshot consisting of transactions at:
     */
    long tInvalid = nowWritePointer - 5;    // t1 - invalid
    long readPtr = nowWritePointer - 4;     // t2 - here and earlier committed
    long tLong = nowWritePointer - 3;       // t3 - in-progress LONG
    long tCommitted = nowWritePointer - 2;  // t4 - committed, changeset (r1, r2)
    long tShort = nowWritePointer - 1;      // t5 - in-progress SHORT, canCommit called, changeset (r3, r4)

    TreeMap<Long, TransactionManager.InProgressTx> inProgress = Maps.newTreeMap(ImmutableSortedMap.of(
        tLong, new TransactionManager.InProgressTx(readPtr,
            TransactionManager.getTxExpirationFromWritePointer(tLong, TxConstants.Manager.DEFAULT_TX_LONG_TIMEOUT),
            TransactionType.LONG),
        tShort, new TransactionManager.InProgressTx(readPtr, now + 1000, TransactionType.SHORT)));

    TransactionSnapshot snapshot = new TransactionSnapshot(now, readPtr, nowWritePointer,
        Lists.newArrayList(tInvalid), // invalid
        inProgress,
        ImmutableMap.<Long, Set<ChangeId>>of(
            tShort, Sets.newHashSet(new ChangeId(new byte[]{'r', '3'}), new ChangeId(new byte[]{'r', '4'}))),
        ImmutableMap.<Long, Set<ChangeId>>of(
            tCommitted, Sets.newHashSet(new ChangeId(new byte[]{'r', '1'}), new ChangeId(new byte[]{'r', '2'}))));

    Configuration conf1 = new Configuration();
    conf1.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, DefaultSnapshotCodec.class.getName());
    SnapshotCodecProvider provider1 = new SnapshotCodecProvider(conf1);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      provider1.encode(out, snapshot);
    } finally {
      out.close();
    }

    TransactionSnapshot snapshot2 = provider1.decode(new ByteArrayInputStream(out.toByteArray()));
    assertEquals(snapshot.getReadPointer(), snapshot2.getReadPointer());
    assertEquals(snapshot.getWritePointer(), snapshot2.getWritePointer());
    assertEquals(snapshot.getInvalid(), snapshot2.getInvalid());
    // in-progress transactions will have missing types
    assertNotEquals(snapshot.getInProgress(), snapshot2.getInProgress());
    assertEquals(snapshot.getCommittingChangeSets(), snapshot2.getCommittingChangeSets());
    assertEquals(snapshot.getCommittedChangeSets(), snapshot2.getCommittedChangeSets());

    // after fixing in-progress, full snapshot should match
    Map<Long, TransactionManager.InProgressTx> fixedInProgress = TransactionManager.txnBackwardsCompatCheck(
        TxConstants.Manager.DEFAULT_TX_LONG_TIMEOUT, 10000L, snapshot2.getInProgress());
    assertEquals(snapshot.getInProgress(), fixedInProgress);
    assertEquals(snapshot, snapshot2);
  }

  /**
   * Test full stack serialization for a TransactionManager migrating from DefaultSnapshotCodec to SnapshotCodecV3.
   */
  @Test
  public void testDefaultToV3Migration() throws Exception {
    File testDir = tmpDir.newFolder("testDefaultToV3Migration");
    Configuration conf = new Configuration();
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, DefaultSnapshotCodec.class.getName());
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());

    Injector injector = Guice.createInjector(new ConfigModule(conf),
        new DiscoveryModules().getSingleNodeModules(), new TransactionModules().getSingleNodeModules());

    TransactionManager txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    txManager.startLong();

    // shutdown to force a snapshot
    txManager.stopAndWait();

    TransactionStateStorage txStorage = injector.getInstance(TransactionStateStorage.class);
    txStorage.startAndWait();

    // confirm that the in-progress entry is missing a type
    TransactionSnapshot snapshot = txStorage.getLatestSnapshot();
    assertNotNull(snapshot);
    assertEquals(1, snapshot.getInProgress().size());
    Map.Entry<Long, TransactionManager.InProgressTx> entry =
        snapshot.getInProgress().entrySet().iterator().next();
    assertNull(entry.getValue().getType());


    // start a new Tx manager to test fixup
    Configuration conf2 = new Configuration();
    conf2.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());
    conf2.setStrings(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES,
        DefaultSnapshotCodec.class.getName(), SnapshotCodecV3.class.getName());
    Injector injector2 = Guice.createInjector(new ConfigModule(conf2),
        new DiscoveryModules().getSingleNodeModules(), new TransactionModules().getSingleNodeModules());

    TransactionManager txManager2 = injector2.getInstance(TransactionManager.class);
    txManager2.startAndWait();

    // state should be recovered
    TransactionSnapshot snapshot2 = txManager2.getCurrentState();
    assertEquals(1, snapshot2.getInProgress().size());
    Map.Entry<Long, TransactionManager.InProgressTx> inProgressTx =
        snapshot2.getInProgress().entrySet().iterator().next();
    assertEquals(TransactionType.LONG, inProgressTx.getValue().getType());

    // save a new snapshot
    txManager2.stopAndWait();

    TransactionStateStorage txStorage2 = injector2.getInstance(TransactionStateStorage.class);
    txStorage2.startAndWait();

    TransactionSnapshot snapshot3 = txStorage2.getLatestSnapshot();
    // full snapshot should have deserialized correctly without any fixups
    assertEquals(snapshot2.getInProgress(), snapshot3.getInProgress());
    assertEquals(snapshot2, snapshot3);
  }

  @Test
  public void testSnapshotCodecProviderConfiguration() throws Exception {
    Configuration conf = new Configuration(false);
    StringBuilder buf = new StringBuilder();
    for (Class c : TxConstants.Persist.DEFAULT_TX_SNAPHOT_CODEC_CLASSES) {
      if (buf.length() > 0) {
        buf.append(",\n    ");
      }
      buf.append(c.getName());
    }
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, buf.toString());

    SnapshotCodecProvider codecProvider = new SnapshotCodecProvider(conf);
    SnapshotCodec v1codec = codecProvider.getCodecForVersion(new DefaultSnapshotCodec().getVersion());
    assertNotNull(v1codec);
    assertTrue(v1codec instanceof DefaultSnapshotCodec);

    SnapshotCodec v2codec = codecProvider.getCodecForVersion(new SnapshotCodecV2().getVersion());
    assertNotNull(v2codec);
    assertTrue(v2codec instanceof SnapshotCodecV2);

    SnapshotCodec v3codec = codecProvider.getCodecForVersion(new SnapshotCodecV3().getVersion());
    assertNotNull(v3codec);
    assertTrue(v3codec instanceof SnapshotCodecV3);

    SnapshotCodec v4codec = codecProvider.getCodecForVersion(new SnapshotCodecV4().getVersion());
    assertNotNull(v4codec);
    assertTrue(v4codec instanceof SnapshotCodecV4);
  }
}
