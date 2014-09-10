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

import co.cask.tephra.TxConstants;
import co.cask.tephra.snapshot.DefaultSnapshotCodec;
import co.cask.tephra.snapshot.SnapshotCodecProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Runs transaction persistence tests against the {@link LocalFileTransactionStateStorage} and
 * {@link LocalFileTransactionLog} implementations.
 */
public class LocalTransactionStateStorageTest extends AbstractTransactionStateStorageTest {
  @ClassRule
  public static TemporaryFolder tmpDir = new TemporaryFolder();

  @Override
  protected Configuration getConfiguration(String testName) throws IOException {
    File testDir = tmpDir.newFolder(testName);
    Configuration conf = HBaseConfiguration.create();
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, DefaultSnapshotCodec.class.getName());

    return conf;
  }

  @Override
  protected AbstractTransactionStateStorage getStorage(Configuration conf) {
    return new LocalFileTransactionStateStorage(conf, new SnapshotCodecProvider(conf));
  }
}
