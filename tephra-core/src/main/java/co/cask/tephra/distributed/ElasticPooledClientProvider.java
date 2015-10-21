/*
 * Copyright © 2012-2015 Cask Data, Inc.
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
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an tx client provider that uses a bounded size pool of connections.
 */
public class ElasticPooledClientProvider extends AbstractClientProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(ElasticPooledClientProvider.class);

  // we will use this as a pool of tx clients
  class TxClientPool extends ElasticPool<TransactionServiceThriftClient, TException> {
    TxClientPool(int sizeLimit) {
      super(sizeLimit);
    }

    @Override
    protected TransactionServiceThriftClient create() throws TException {
      return newClient();
    }

    @Override
    protected void destroy(TransactionServiceThriftClient client) {
      client.close();
    }
  }

  // we will use this as a pool of tx clients
  private volatile TxClientPool clients;

  // the limit for the number of active clients
  private int maxClients;

  public ElasticPooledClientProvider(Configuration conf, DiscoveryServiceClient discoveryServiceClient) {
    super(conf, discoveryServiceClient);
  }

  private void initializePool() throws TException {
    // initialize the super class (needed for service discovery)
    super.initialize();

    // create a (empty) pool of tx clients
    maxClients = configuration.getInt(TxConstants.Service.CFG_DATA_TX_CLIENT_COUNT,
        TxConstants.Service.DEFAULT_DATA_TX_CLIENT_COUNT);
    if (maxClients < 1) {
      LOG.warn("Configuration of " + TxConstants.Service.CFG_DATA_TX_CLIENT_COUNT +
                                 " is invalid: value is " + maxClients + " but must be at least 1. " +
                                 "Using 1 as a fallback. ");
      maxClients = 1;
    }
    this.clients = new TxClientPool(maxClients);
  }

  @Override
  public TransactionServiceThriftClient getClient() throws TException {
    return getClientPool().obtain();
  }

  @Override
  public void returnClient(TransactionServiceThriftClient client) {
    getClientPool().release(client);
  }

  @Override
  public void discardClient(TransactionServiceThriftClient client) {
    getClientPool().discard(client);
  }

  @Override
  public String toString() {
    return "Elastic pool of size " + this.maxClients;
  }

  private TxClientPool getClientPool() {
    if (clients != null) {
      return clients;
    }

    synchronized (this) {
      if (clients == null) {
        try {
          initializePool();
        } catch (TException e) {
          LOG.error("Failed to initialize Tx client provider", e);
          throw Throwables.propagate(e);
        }
      }
    }
    return clients;
  }
}
