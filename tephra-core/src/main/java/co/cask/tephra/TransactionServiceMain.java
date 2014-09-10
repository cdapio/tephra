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

import co.cask.tephra.distributed.TransactionService;
import co.cask.tephra.runtime.ConfigModule;
import co.cask.tephra.runtime.DiscoveryModules;
import co.cask.tephra.runtime.TransactionClientModule;
import co.cask.tephra.runtime.TransactionModules;
import co.cask.tephra.runtime.ZKModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.common.Services;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

/**
 * Driver class to start and stop tx in distributed mode.
 */
public class TransactionServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceMain.class);

  private Configuration conf;
  private TransactionService txService;

  public static void main(String args[]) throws Exception {
    TransactionServiceMain instance = new TransactionServiceMain();
    instance.doMain(args);
  }

  public TransactionServiceMain() {
    this(null);
  }

  public TransactionServiceMain(Configuration conf) {
    this.conf = conf;
  }

  /**
   * The main method. It simply call methods in the same sequence
   * as if the program is started by jsvc.
   */
  public void doMain(final String[] args) throws Exception {
    final CountDownLatch shutdownLatch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
      try {
        try {
          TransactionServiceMain.this.stop();
        } finally {
          try {
            TransactionServiceMain.this.destroy();
          } finally {
            shutdownLatch.countDown();
          }
        }
      } catch (Throwable t) {
        LOG.error("Exception when shutting down: " + t.getMessage(), t);
      }
      }
    });
    init(args);
    start();

    shutdownLatch.await();
  }

  /**
   * Invoked by jsvc to initialize the program.
   */
  public void init(String[] args) {
    if (conf == null) {
      conf = HBaseConfiguration.create();
    }
  }

  /**
   * Invoked by jsvc to start the program.
   */
  public void start() throws Exception {
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new ZKModule(),
      new DiscoveryModules().getDistributedModules(),
      new TransactionModules().getDistributedModules(),
      new TransactionClientModule()
    );

    ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    // start a tx server
    txService = injector.getInstance(TransactionService.class);
    Future<?> future = Services.getCompletionFuture(txService);
    try {
      txService.start();
    } catch (Exception e) {
      System.err.println("Failed to start service: " + e.getMessage());
    }
    future.get();
  }

  /**
   * Invoked by jsvc to stop the program.
   */
  public void stop() {
    if (txService == null) {
      return;
    }
    try {
      if (txService.isRunning()) {
        txService.stopAndWait();
      }
    } catch (Throwable e) {
      LOG.error("Failed to shutdown transaction service.", e);
      // because shutdown hooks execute concurrently, the logger may be closed already: thus also print it.
      System.err.println("Failed to shutdown transaction service: " + e.getMessage());
      e.printStackTrace(System.err);
    }
  }

  /**
   * Invoked by jsvc for resource cleanup.
   */
  public void destroy() { }

}
