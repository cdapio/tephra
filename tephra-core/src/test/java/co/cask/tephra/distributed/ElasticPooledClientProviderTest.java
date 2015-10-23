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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ElasticPooledClientProviderTest extends ClientProviderTestBase {

  public static final int MAX_CLIENT_COUNT = 3;

  @Override
  protected void configureConf(Configuration conf) {
    conf.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_COUNT, MAX_CLIENT_COUNT);
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

    final ElasticPooledClientProvider clientProvider = new ElasticPooledClientProvider(conf,
      injector.getInstance(DiscoveryServiceClient.class));

    //Now race to get MAX_CLIENT_COUNT+1 clients, exhausting the pool and requesting 1 more.
    List<Future<Integer>> clientIds = new ArrayList<Future<Integer>>();
    ExecutorService executor = Executors.newFixedThreadPool(MAX_CLIENT_COUNT + 1);
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


  protected static class RetrieveClient implements Callable<Integer> {
    private final ThriftClientProvider pool;

    public RetrieveClient(ThriftClientProvider pool) {
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
