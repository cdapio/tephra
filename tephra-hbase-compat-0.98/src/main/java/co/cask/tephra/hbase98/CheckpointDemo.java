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

package co.cask.tephra.hbase98;

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.distributed.TransactionServiceClient;
import co.cask.tephra.hbase98.coprocessor.TransactionProcessor;
import co.cask.tephra.runtime.ConfigModule;
import co.cask.tephra.runtime.DiscoveryModules;
import co.cask.tephra.runtime.TransactionClientModule;
import co.cask.tephra.runtime.TransactionModules;
import co.cask.tephra.runtime.ZKModule;
import co.cask.tephra.util.ConfigurationFactory;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CheckpointDemo {
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointDemo.class);

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] COL = Bytes.toBytes("c");
  private static final byte[] VAL = Bytes.toBytes("1");
  private static final byte[][] SPLIT_KEYS =
      { Bytes.toBytes(1), Bytes.toBytes(2), Bytes.toBytes(3), Bytes.toBytes(4) };

  public static void main(String[] args) {
    if (args.length < 1 || args.length > 2) {
      System.err.println("Usage: java " + CheckpointDemo.class.getName() + " <tablename> [-c]");
      System.exit(1);
    }
    String tableString = args[0];
    boolean checkpoint = false;
    if (args.length > 1) {
      checkpoint = "-c".equals(args[1]);
    }
    Configuration conf = new ConfigurationFactory().get();
    Injector injector = Guice.createInjector(
        new ConfigModule(conf),
        new ZKModule(),
        new DiscoveryModules().getDistributedModules(),
        new TransactionModules().getDistributedModules(),
        new TransactionClientModule()
    );
    LOG.info("Running tx checkpoint demo.");

    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    HTable table = null;
    try {
      TableName tableName = TableName.valueOf(tableString);
      TransactionServiceClient client = injector.getInstance(TransactionServiceClient.class);

      createTable(conf, tableName);

      table = new HTable(conf, tableName);
      TransactionAwareHTable txTable = new TransactionAwareHTable(table);

      TransactionContext txContext = new TransactionContext(client, txTable);

      txContext.start();
      LOG.info("Tx write pointer={}", txContext.getCurrentTransaction().getWritePointer());
      LOG.info("Loading 5 initial records");
      for (int i = 0; i < 5; i++) {
        txTable.put(new Put(Bytes.toBytes(i)).add(FAMILY, COL, VAL));
        TimeUnit.SECONDS.sleep(1);
      }

      if (checkpoint) {
        LOG.info("Performing checkpoint");
        txContext.checkpoint();
        Transaction tx = txContext.getCurrentTransaction();
        tx.setVisibility(Transaction.VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
        LOG.info("Tx write pointer={}", txContext.getCurrentTransaction().getWritePointer());
      }

      LOG.info("Starting scan + insert over same table");
      List<Scan> scans = new ArrayList<Scan>();
      List<HRegionLocation> allRegions = table.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      for (HRegionLocation loc : allRegions) {
        scans.add(new Scan(loc.getRegionInfo().getStartKey(), loc.getRegionInfo().getEndKey()));
      }
      for (Scan scan : scans) {
        LOG.info("Scanning from " + Bytes.toStringBinary(scan.getStartRow())
            + " to " + Bytes.toStringBinary(scan.getStopRow()));
        ResultScanner scanner = txTable.getScanner(scan);
        for (Result row : scanner) {
          int readRow = Bytes.toInt(row.getRow());
          long readTs = row.getColumnLatestCell(FAMILY, COL).getTimestamp();
          int rowToWrite = readRow * 10;
          LOG.info("Inserting row={} for read row: key={}, ts={}", rowToWrite, readRow, readTs);
          txTable.put(new Put(Bytes.toBytes(rowToWrite)).add(FAMILY, COL, VAL));
        }
      }
      txContext.finish();

    } catch (Exception e) {
      LOG.error("Failed on: " + e.getMessage(), e);
    } finally {
      if (table != null) {
        try { table.close(); } catch (IOException ignored) { }
      }
      zkClient.stopAndWait();
    }
  }

  private static void createTable(Configuration conf, TableName tableName) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      if (!admin.tableExists(tableName)) {
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        tableDesc.addCoprocessor(TransactionProcessor.class.getName());
        HColumnDescriptor columnDesc = new HColumnDescriptor(FAMILY);
        columnDesc.setMaxVersions(Integer.MAX_VALUE);
        tableDesc.addFamily(columnDesc);
        admin.createTable(tableDesc, SPLIT_KEYS);
      }
    } finally {
      admin.close();
    }
  }
}
