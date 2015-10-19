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

package co.cask.tephra.distributed;

import co.cask.tephra.ThriftTransactionSystemTest;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
import co.cask.tephra.persist.InMemoryTransactionStateStorage;
import co.cask.tephra.persist.TransactionEdit;
import co.cask.tephra.persist.TransactionLog;
import co.cask.tephra.persist.TransactionStateStorage;
import co.cask.tephra.runtime.ConfigModule;
import co.cask.tephra.runtime.DiscoveryModules;
import co.cask.tephra.runtime.TransactionClientModule;
import co.cask.tephra.runtime.TransactionModules;
import co.cask.tephra.runtime.ZKModule;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This tests whether transaction service hangs on stop when heavily loaded - https://issues.cask.co/browse/TEPHRA-132
 */
public class ThriftTransactionServerTest {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftTransactionSystemTest.class);

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClientService;
  private static TransactionService txService;
  private static TransactionStateStorage storage;
  static Injector injector;

  private static final int NUM_CLIENTS = 17;
  private static final CountDownLatch STORAGE_WAIT_LATCH = new CountDownLatch(1);
  private static final CountDownLatch CLIENTS_DONE_LATCH = new CountDownLatch(NUM_CLIENTS);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void start() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    Configuration conf = new Configuration();
    conf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, false);
    conf.set(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM, zkServer.getConnectionStr());
    conf.set(TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY, "n-times");
    conf.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, 1);
    conf.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_COUNT, NUM_CLIENTS);
    conf.setLong(TxConstants.Service.CFG_DATA_TX_CLIENT_TIMEOUT, TimeUnit.HOURS.toMillis(1));
    conf.setInt(TxConstants.Service.CFG_DATA_TX_SERVER_IO_THREADS, 2);
    conf.setInt(TxConstants.Service.CFG_DATA_TX_SERVER_THREADS, 4);

    injector = Guice.createInjector(
      new ConfigModule(conf),
      new ZKModule(),
      new DiscoveryModules().getDistributedModules(),
      Modules.override(new TransactionModules().getDistributedModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(TransactionStateStorage.class).to(SlowTransactionStorage.class).in(Scopes.SINGLETON);
          }
        }),
      new TransactionClientModule()
    );

    zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    // start a tx server
    txService = injector.getInstance(TransactionService.class);
    storage = injector.getInstance(TransactionStateStorage.class);
    try {
      LOG.info("Starting transaction service");
      txService.startAndWait();
    } catch (Exception e) {
      LOG.error("Failed to start service: ", e);
    }
  }

  @Before
  public void reset() throws Exception {
    getClient().resetState();
  }

  @AfterClass
  public static void stop() throws Exception {
    txService.stopAndWait();
    storage.stopAndWait();
    zkClientService.stopAndWait();
    zkServer.stopAndWait();
  }

  public TransactionSystemClient getClient() throws Exception {
    return injector.getInstance(TransactionSystemClient.class);
  }

  @Test
  public void testThriftServerStop() throws Exception {
    int nThreads = NUM_CLIENTS;
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
    for (int i = 0; i < nThreads; ++i) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            TransactionSystemClient txClient = getClient();
            CLIENTS_DONE_LATCH.countDown();
            txClient.startShort();
          } catch (Exception e) {
            // Exception expected
          }
        }
      });
    }

    // Wait till all clients finish sending reqeust to transaction manager
    CLIENTS_DONE_LATCH.await();
    TimeUnit.SECONDS.sleep(1);

    // Expire zookeeper session, which causes Thrift server to stop.
    expireZkSession(zkClientService);
    waitForThriftTermination();

    // Stop Zookeeper client so that it does not re-connect to Zookeeper and start Thrift sever again.
    zkClientService.stopAndWait();
    STORAGE_WAIT_LATCH.countDown();
    TimeUnit.SECONDS.sleep(1);

    // Make sure Thrift server stopped.
    Assert.assertEquals(Service.State.TERMINATED, txService.thriftRPCServerState());
  }

  private void expireZkSession(ZKClientService zkClientService) throws Exception {
    ZooKeeper zooKeeper = zkClientService.getZooKeeperSupplier().get();
    final SettableFuture<?> connectFuture = SettableFuture.create();
    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
          connectFuture.set(null);
        }
      }
    };

    // Create another Zookeeper session with the same sessionId so that the original one expires.
    final ZooKeeper dupZookeeper =
      new ZooKeeper(zkClientService.getConnectString(), zooKeeper.getSessionTimeout(), watcher,
                    zooKeeper.getSessionId(), zooKeeper.getSessionPasswd());
    connectFuture.get(30, TimeUnit.SECONDS);
    Assert.assertEquals("Failed to re-create current session", dupZookeeper.getState(), ZooKeeper.States.CONNECTED);
    dupZookeeper.close();
  }

  private void waitForThriftTermination() throws InterruptedException {
    int count = 0;
    while (txService.thriftRPCServerState() != Service.State.TERMINATED && count++ < 200) {
      TimeUnit.MILLISECONDS.sleep(50);
    }
  }

  private static class SlowTransactionStorage extends InMemoryTransactionStateStorage {
    @Override
    public TransactionLog createLog(long timestamp) throws IOException {
      return new SlowTransactionLog(timestamp);
    }
  }

  private static class SlowTransactionLog extends InMemoryTransactionStateStorage.InMemoryTransactionLog {
    public SlowTransactionLog(long timestamp) {
      super(timestamp);
    }

    @Override
    public void append(TransactionEdit edit) throws IOException {
      try {
        STORAGE_WAIT_LATCH.await();
      } catch (InterruptedException e) {
        LOG.error("Got exception: ", e);
      }
      super.append(edit);
    }

    @Override
    public void append(List<TransactionEdit> edits) throws IOException {
      try {
        STORAGE_WAIT_LATCH.await();
      } catch (InterruptedException e) {
        LOG.error("Got exception: ", e);
      }
      super.append(edits);
    }
  }
}
