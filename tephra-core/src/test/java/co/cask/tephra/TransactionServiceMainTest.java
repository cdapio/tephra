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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test for verifying TransactionServiceMain works correctly.
 */
public class TransactionServiceMainTest {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceMainTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  public static Thread.State toThreadState(int var0) {
    return (var0 & 4) != 0? Thread.State.RUNNABLE:((var0 & 1024) != 0? Thread.State.BLOCKED:((var0 & 16) != 0? Thread.State.WAITING:((var0 & 32) != 0? Thread.State.TIMED_WAITING:((var0 & 2) != 0? Thread.State.TERMINATED:((var0 & 1) == 0? Thread.State.NEW: Thread.State.RUNNABLE)))));
  }

  @Test
  public void testClientServer() throws Exception {
    // Simply start a transaction server and connect to it with the client.
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    try {
      final Configuration conf = new Configuration();
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
        ExecutorService executorService = Executors.newFixedThreadPool(15);
        for (int i = 0; i < 15; ++i) {
          final int finalI = i;
          executorService.submit(new Runnable() {
            @Override
            public void run() {
              try {
                LOG.error("############## Starting client " + finalI);
                TransactionServiceClient.doMain(true, conf);
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          });
        }
      } finally {
        LOG.error("############### Waiting for 10 seconds");
        TimeUnit.SECONDS.sleep(10);

        main.stop();
        t.join();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }
}
