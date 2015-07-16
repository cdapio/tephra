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
package co.cask.tephra.distributed;

import co.cask.tephra.TransactionServiceMain;
import co.cask.tephra.TxConstants;
import co.cask.tephra.runtime.ConfigModule;
import co.cask.tephra.runtime.DiscoveryModules;
import co.cask.tephra.runtime.TransactionClientModule;
import co.cask.tephra.runtime.TransactionModules;
import co.cask.tephra.runtime.ZKModule;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PooledClientProviderTest {

  public static final int MAX_CLIENT_COUNT = 3;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testClientConnectionPoolMaximumNumberOfClients() throws Exception {
    // We need a server for the client to connect to
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    try {
      Configuration conf = new Configuration();
      conf.set(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM, zkServer.getConnectionStr());
      conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, tmpFolder.newFolder().getAbsolutePath());
      conf.set("data.tx.client.count", Integer.toString(MAX_CLIENT_COUNT));

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

        startClientAndTestPool(conf);
      } finally {
        main.stop();
        t.join();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }

  private void startClientAndTestPool(Configuration conf) throws InterruptedException, ExecutionException {
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new ZKModule(),
      new DiscoveryModules().getDistributedModules(),
      new TransactionModules().getDistributedModules(),
      new TransactionClientModule()
    );

    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    final PooledClientProvider clientProvider = new PooledClientProvider(conf,
      injector.getInstance(DiscoveryServiceClient.class));

    //Now race to get MAX_CLIENT_COUNT+1 clients, exhausting the pool and requesting 1 more.
    List<Future<Integer>> clientIds = new ArrayList<Future<Integer>>();
    ExecutorService executor = Executors.newFixedThreadPool(MAX_CLIENT_COUNT + 2);
    for (int i = 0; i < MAX_CLIENT_COUNT + 1; i++) {
      clientIds.add(executor.submit(new RetrieveClient(clientProvider)));
    }

    Set<Integer> ids = new HashSet<Integer>();
    for (Future<Integer> id : clientIds) {
      ids.add(id.get());
    }
    executor.shutdown();
    Assert.assertEquals(MAX_CLIENT_COUNT, ids.size());
  }

  private static class RetrieveClient implements Callable<Integer> {
    private final PooledClientProvider pool;

    public RetrieveClient(PooledClientProvider pool) {
      this.pool = pool;
    }

    @Override
    public Integer call() throws Exception {
      TransactionServiceThriftClient client = pool.getClient();
      int id = System.identityHashCode(client);
      try {
        //"use" the client
        Thread.sleep(100);
      } finally {
        pool.returnClient(client);
      }
      return id;
    }
  }
}
