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
import co.cask.tephra.metrics.TxMetricsCollector;
import co.cask.tephra.snapshot.SnapshotCodecProvider;
import co.cask.tephra.snapshot.SnapshotCodecV2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;


/**
 * Tests persistence of transaction snapshots and write-ahead logs to HDFS storage, using the
 * {@link HDFSTransactionStateStorage} and {@link HDFSTransactionLog} implementations.
 */
public class HDFSTransactionStateStorageTest extends AbstractTransactionStateStorageTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static MiniDFSCluster dfsCluster;
  private static Configuration conf;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.newFolder().getAbsolutePath());

    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    conf = new Configuration(dfsCluster.getFileSystem().getConf());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    dfsCluster.shutdown();
  }

  @Override
  protected Configuration getConfiguration(String testName) throws IOException {
    // tests should use the current user for HDFS
    conf.unset(TxConstants.Manager.CFG_TX_HDFS_USER);
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, tmpFolder.newFolder().getAbsolutePath());
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, SnapshotCodecV2.class.getName());
    return conf;
  }

  @Override
  protected AbstractTransactionStateStorage getStorage(Configuration conf) {
    return new HDFSTransactionStateStorage(conf, new SnapshotCodecProvider(conf), new TxMetricsCollector());
  }


  @Test
  public void testTxEdits() throws Exception {
    HDFSTransactionStateStorage
      hdfsTransactionStateStorage = (HDFSTransactionStateStorage) getStorage(getConfiguration("testingEdits"));
    hdfsTransactionStateStorage.startAndWait();
    TransactionLog transactionLog = hdfsTransactionStateStorage.getTransactionLog(
      new Path("/Users/shankar/tx.snapshot/latest/tx.snapshot/txlog.1443792213636"), 1443792213636L);
    TransactionLogReader reader = transactionLog.getReader();
    TransactionEdit transactionEdit;
    int count = 0;
    while((transactionEdit = reader.next()) != null) {
      System.out.println(count++);
//      if (count > 118355) {
//        System.out.println(transactionEdit);
//      }
    }
  }
}
