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

package co.cask.tephra.examples;

import co.cask.tephra.TxConstants;
import co.cask.tephra.distributed.TransactionService;
import co.cask.tephra.persist.InMemoryTransactionStateStorage;
import co.cask.tephra.persist.TransactionStateStorage;
import co.cask.tephra.runtime.ConfigModule;
import co.cask.tephra.runtime.DiscoveryModules;
import co.cask.tephra.runtime.TransactionClientModule;
import co.cask.tephra.runtime.TransactionModules;
import co.cask.tephra.runtime.ZKModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.yarn.lib.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link BalanceBooks} program.
 */
public class BalanceBooksTest {
  private static final Logger LOG = LoggerFactory.getLogger(BalanceBooksTest.class);
  private static HBaseTestingUtility testUtil;
  private static TransactionService txService;
  private static ZKClientService zkClientService;

  @BeforeClass
  public static void setup() throws Exception {
    testUtil = new HBaseTestingUtility();
    Configuration conf = testUtil.getConfiguration();
    conf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, false);
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, "/tx.snapshot");
    testUtil.startMiniCluster();

    String zkClusterKey = testUtil.getClusterKey(); // hostname:clientPort:parentZnode
    String zkQuorum = zkClusterKey.substring(0, zkClusterKey.lastIndexOf(':'));
    LOG.info("Zookeeper Quorum is running at {}", zkQuorum);
    conf.set(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM, zkQuorum);

    Injector injector = Guice.createInjector(
        new ConfigModule(conf),
        new ZKModule(),
        new DiscoveryModules().getDistributedModules(),
        Modules.override(new TransactionModules().getDistributedModules())
            .with(new AbstractModule() {
              @Override
              protected void configure() {
                bind(TransactionStateStorage.class).to(InMemoryTransactionStateStorage.class).in(Scopes.SINGLETON);
              }
            }),
        new TransactionClientModule()
    );

    zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    // start a tx server
    txService = injector.getInstance(TransactionService.class);
    try {
      LOG.info("Starting transaction service");
      txService.startAndWait();
    } catch (Exception e) {
      LOG.error("Failed to start service: ", e);
    }

  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (txService != null) {
      txService.stopAndWait();
    }
    if (zkClientService != null) {
      zkClientService.stopAndWait();
    }
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void testBalanceBooks() throws Exception {
    BalanceBooks bb = new BalanceBooks(5, 100, testUtil.getConfiguration());
    try {
      bb.init();
      bb.run();
      assertTrue(bb.verify());
    } finally {
      bb.close();
    }
  }
}
