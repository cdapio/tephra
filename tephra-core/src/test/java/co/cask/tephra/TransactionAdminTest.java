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

package co.cask.tephra;

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
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

public class TransactionAdminTest {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionAdminTest.class);
  
  private static Configuration conf;
  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClientService;
  private static TransactionService txService;
  private static TransactionSystemClient txClient;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void start() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    conf = new Configuration();
    conf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, false);
    conf.set(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM, zkServer.getConnectionStr());
    conf.set(TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY, "n-times");
    conf.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, 1);

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
    txClient = injector.getInstance(TransactionSystemClient.class);
    try {
      LOG.info("Starting transaction service");
      txService.startAndWait();
    } catch (Exception e) {
      LOG.error("Failed to start service: ", e);
    }
  }

  @Before
  public void reset() throws Exception {
    txClient.resetState();
  }

  @AfterClass
  public static void stop() throws Exception {
    txService.stopAndWait();
    zkClientService.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test
  public void testPrintUsage() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    TransactionAdmin txAdmin = new TransactionAdmin(new PrintStream(out), new PrintStream(err));
    int status = txAdmin.doMain(new String[0], conf);
    Assert.assertEquals(1, status);
    //noinspection ConstantConditions
    Assert.assertTrue(err.toString("UTF-8").startsWith("Usage:"));
    Assert.assertEquals(0, out.toByteArray().length);
  }
  
  @Test
  public void testTruncateInvalidTx() throws Exception {
    Transaction tx1 = txClient.startLong();
    Transaction tx2 = txClient.startShort();
    txClient.invalidate(tx1.getTransactionId());
    txClient.invalidate(tx2.getTransactionId());
    Assert.assertEquals(2, txClient.getInvalidSize());

    TransactionAdmin txAdmin = new TransactionAdmin(new PrintStream(System.out), new PrintStream(System.err));
    int status = txAdmin.doMain(new String[]{"--truncate-invalid-tx", String.valueOf(tx2.getTransactionId())}, conf);
    Assert.assertEquals(0, status);
    Assert.assertEquals(1, txClient.getInvalidSize());
  }

  @Test
  public void testTruncateInvalidTxBefore() throws Exception {
    Transaction tx1 = txClient.startLong();
    TimeUnit.MILLISECONDS.sleep(1);
    long beforeTx2 = System.currentTimeMillis();
    Transaction tx2 = txClient.startLong();

    // Try before invalidation
    Assert.assertEquals(0, txClient.getInvalidSize());
    TransactionAdmin txAdmin = new TransactionAdmin(new PrintStream(System.out), new PrintStream(System.err));
    int status = txAdmin.doMain(new String[]{"--truncate-invalid-tx-before", String.valueOf(beforeTx2)}, conf);
    // Assert command failed due to in-progress transactions
    Assert.assertEquals(1, status);
    // Assert no change to invalid size
    Assert.assertEquals(0, txClient.getInvalidSize());

    txClient.invalidate(tx1.getTransactionId());
    txClient.invalidate(tx2.getTransactionId());
    Assert.assertEquals(2, txClient.getInvalidSize());

    status = txAdmin.doMain(new String[]{"--truncate-invalid-tx-before", String.valueOf(beforeTx2)}, conf);
    Assert.assertEquals(0, status);
    Assert.assertEquals(1, txClient.getInvalidSize());
  }

  @Test
  public void testGetInvalidTxSize() throws Exception {
    Transaction tx1 = txClient.startShort();
    txClient.startLong();
    txClient.invalidate(tx1.getTransactionId());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    TransactionAdmin txAdmin = new TransactionAdmin(new PrintStream(out), new PrintStream(err));
    int status = txAdmin.doMain(new String[]{"--get-invalid-tx-size"}, conf);
    Assert.assertEquals(0, status);
    //noinspection ConstantConditions
    Assert.assertTrue(out.toString("UTF-8").contains("Invalid list size: 1\n"));
  }
}
