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

import co.cask.tephra.distributed.TransactionServiceClient;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.CountDownLatch;

/**
 * Test for verifying TransactionServiceMain works correctly.
 */
public class TransactionServiceMainTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testClientServer() throws Exception {
    // Simply start a transaction server and connect to it with the client.
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    try {
      Configuration conf = new Configuration();
      conf.set(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM, zkServer.getConnectionStr());
      conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, tmpFolder.newFolder().getAbsolutePath());

      final TransactionServiceMain main = new TransactionServiceMain(conf);
      final CountDownLatch latch = new CountDownLatch(1);
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            main.start();
            latch.countDown();
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };

      try {
        t.start();
        // Wait for service to startup
        latch.await();
        TransactionServiceClient.doMain(new TransactionServiceClient.Options(), true, conf);
      } finally {
        main.stop();
        t.join();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }
}
