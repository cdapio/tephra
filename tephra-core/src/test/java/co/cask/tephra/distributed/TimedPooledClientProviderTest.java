/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.tephra.TxConstants;
import co.cask.tephra.runtime.ConfigModule;
import co.cask.tephra.runtime.DiscoveryModules;
import co.cask.tephra.runtime.TransactionClientModule;
import co.cask.tephra.runtime.TransactionModules;
import co.cask.tephra.runtime.ZKModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TimedPooledClientProviderTest extends ClientProviderTestBase {

  public static final long POOL_TIMEOUT = TimeUnit.SECONDS.toMillis(1);

  @Override
  protected void configureConf(Configuration conf) {
    conf.setLong(TxConstants.Service.CFG_DATA_TX_CLIENT_POOL_TIMEOUT, POOL_TIMEOUT);
  }

  // tests ClientConnectionPool maximum number of clients
  protected void startClientAndTestPool(Configuration conf) throws Exception {
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new ZKModule(),
      new DiscoveryModules().getDistributedModules(),
      new TransactionModules().getDistributedModules(),
      new TransactionClientModule()
    );

    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    final TimedPooledClientProvider clientProvider = new TimedPooledClientProvider(conf,
      injector.getInstance(DiscoveryServiceClient.class));


    int numThreads = 5;

    List<Future<TransactionServiceThriftClient>> clientFutures = new ArrayList<>();
    List<TransactionServiceThriftClient> clients = new ArrayList<>();

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      clientFutures.add(executor.submit(new RetrieveClient(clientProvider)));
    }

    for (Future<TransactionServiceThriftClient> clientFuture : clientFutures) {
      clients.add(clientFuture.get());
    }

    // introduce a delay, so that the clients qualify for cleanup
    TimeUnit.MILLISECONDS.sleep(POOL_TIMEOUT);

    // trigger a cleanup, which happens in TimedObjectPool#obtain()
    clientProvider.getClient();

    int numOpenClients = 0;
    // the first four should have been destroyed (and therefore closed)
    for (int i = 0; i < numThreads; i++) {
      if (clients.get(i).transport.isOpen()) {
        numOpenClients++;
      }
    }
    // only one client is still open (not destroyed/cleaned up), because it was returned
    // in the clientProvider.getClient() call, which was used to trigger the cleanup
    Assert.assertEquals(1, numOpenClients);

    executor.shutdown();
  }


  protected static class RetrieveClient implements Callable<TransactionServiceThriftClient> {
    private final ThriftClientProvider pool;

    public RetrieveClient(ThriftClientProvider pool) {
      this.pool = pool;
    }

    @Override
    public TransactionServiceThriftClient call() throws Exception {
      TransactionServiceThriftClient client = pool.getClient();
      try {
        //"use" the client
        Thread.sleep(100);
      } finally {
        pool.returnClient(client);
      }
      return client;
    }
  }
}
