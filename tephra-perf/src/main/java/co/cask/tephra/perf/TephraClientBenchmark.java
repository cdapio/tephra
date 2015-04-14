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

package co.cask.tephra.perf;

import co.cask.performance.bench.api.Benchmark;
import co.cask.performance.bench.api.MetricsClient;
import co.cask.performance.bench.api.SimpleTask;
import co.cask.performance.bench.api.Stage;
import co.cask.performance.bench.api.TaskContext;
import co.cask.performance.bench.api.TaskGroup;
import co.cask.performance.bench.runner.twill.TwillBenchmarkRunner;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionNotInProgressException;
import co.cask.tephra.TxConstants;
import co.cask.tephra.distributed.TransactionServiceClient;
import co.cask.tephra.runtime.ConfigModule;
import co.cask.tephra.runtime.DiscoveryModules;
import co.cask.tephra.runtime.TransactionClientModule;
import co.cask.tephra.runtime.TransactionModules;
import co.cask.tephra.runtime.ZKModule;
import co.cask.tephra.util.ConfigurationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TephraClientBenchmark extends Benchmark {
  private static final Configuration conf = HBaseConfiguration.create();
  private int instances = 1;

  @Override
  public Map<String, String> configure(Map<String, String> config) {

    Map<String, String> newArgs = new HashMap<String, String>();
    copyConfToArgs(conf, newArgs);
    // override with any explicitly provided configs
    newArgs.putAll(config);

    // number of instances is needed to start the benchmark
    String instancesArg = config.get("instances");
    if (instancesArg != null) {
      instances = Integer.parseInt(instancesArg);
      Preconditions.checkArgument(instances > 0, "--instances must be > 0");
    }
    String zkArg = config.get("zk");
    if (zkArg == null) {
      zkArg = conf.get("hbase.zookeeper.quorum");
      newArgs.put("zk", zkArg);
    }

    return newArgs;
  }

  /**
   * Copies all Tephra config properties into the given arguments map.
   */
  private static void copyConfToArgs(Configuration conf, Map<String, String> args) {
    Iterator<Map.Entry<String, String>> entryIter = conf.iterator();
    while (entryIter.hasNext()) {
      Map.Entry<String, String> entry = entryIter.next();
      if (entry.getKey().startsWith("data.tx")) {
        // all copied entries are prefixed with "conf." so we can identify and extract them on the other end
        args.put("conf." + entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public List<Stage> getStages() {
    return Collections.singletonList(new Stage(Collections.singletonList(
        new TaskGroup("tephraClient", TephraClientTask.class, instances)
    )));
  }

  public static void main(String[] args) {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--benchmark";
    args1[args.length + 1] = TephraClientBenchmark.class.getName();
    System.out.println("Running TwillBenchmarkRunner(" + Arrays.toString(args1) + ")");
    TwillBenchmarkRunner.main(args1);
  }

  /**
   * Benchmark task
   */
  public static class TephraClientTask extends SimpleTask {
    private static final Logger LOG = LoggerFactory.getLogger(TephraClientTask.class);

    private Configuration conf;
    private ZKClientService zkClient;
    private TransactionServiceClient client;
    // whether or not the transaction client should call canCommit for the transaction
    private boolean doCanCommit;

    @Override
    public void configure(TaskContext context) {
      super.configure(context);

      //conf = new ConfigurationFactory().get();
      conf = new Configuration();
      Map<String, String> contextArgs = context.getConfig();
      // repopulate any config parameters
      for (Map.Entry<String, String> entry : contextArgs.entrySet()) {
        if (entry.getKey().startsWith("conf.")) {
          conf.set(entry.getKey().substring(5), entry.getValue());
        }
      }

      if (contextArgs.get("zk") != null) {
        conf.set(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM, contextArgs.get("zk"));
      }
      if (contextArgs.containsKey("doCanCommit")) {
        doCanCommit = Boolean.parseBoolean(contextArgs.get("doCanCommit"));
        LOG.info("Set doCanCommit to " + doCanCommit);
      }

      Injector injector = Guice.createInjector(
          new ConfigModule(conf),
          new ZKModule(),
          new DiscoveryModules().getDistributedModules(),
          new TransactionModules().getDistributedModules(),
          new TransactionClientModule()
      );

      zkClient = injector.getInstance(ZKClientService.class);
      zkClient.startAndWait();
      client = injector.getInstance(TransactionServiceClient.class);
    }

    @Override
    public boolean continueOnError() {
      return false;
    }

    @Override
    public String getDescription() {
      return "Tephra client benchmark";
    }

    @Override
    public void runOnce() {
      MetricsClient metricsClient = getMetrics();
      try {
        Transaction tx = client.startShort();
        boolean canCommit = true;
        if (doCanCommit) {
          canCommit = client.canCommit(tx, Collections.<byte[]>emptyList());
        }
        if (canCommit) {
          boolean committed = client.commit(tx);
          if (!committed) {
            client.abort(tx);
            metricsClient.increment("aborted");
          } else {
            metricsClient.increment("committed");
          }
        } else {
          client.abort(tx);
          metricsClient.increment("aborted");
        }
      } catch (TransactionNotInProgressException tnipe) {
        metricsClient.increment("timedout");
        Throwables.propagate(tnipe);
      }
    }

    @Override
    public void stop() {
      zkClient.stopAndWait();
      super.stop();
    }
  }
}
