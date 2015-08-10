.. ==================
.. Cask Tephra
.. ==================

|(Tephra)|

**Transactions for Apache HBase** |(TM)|:
Cask Tephra provides globally consistent transactions on top of Apache HBase.  While HBase
provides strong consistency with row- or region-level ACID operations, it sacrifices
cross-region and cross-table consistency in favor of scalability.  This trade-off requires
application developers to handle the complexity of ensuring consistency when their modifications
span region boundaries.  By providing support for global transactions that span regions, tables, or
multiple RPCs, Tephra simplifies application development on top of HBase, without a significant
impact on performance or scalability for many workloads.

How It Works
------------

Tephra leverages HBase's native data versioning to provide multi-versioned concurrency
control (MVCC) for transactional reads and writes.  With MVCC capability, each transaction
sees its own consistent "snapshot" of data, providing `snapshot isolation 
<http://en.wikipedia.org/wiki/Snapshot_isolation>`__ of concurrent transactions.

Tephra consists of three main components:

- **Transaction Server** - maintains global view of transaction state, assigns new transaction IDs
  and performs conflict detection;
- **Transaction Client** - coordinates start, commit, and rollback of transactions; and
- **TransactionProcessor Coprocessor** - applies filtering to the data read (based on a 
  given transaction's state) and cleans up any data from old (no longer visible) transactions.

Transaction Server
..................

A central transaction manager generates a globally unique, time-based transaction ID for each
transaction that is started, and maintains the state of all in-progress and recently committed
transactions for conflict detection.  While multiple transaction server instances can be run
concurrently for automatic failover, only one server instance is actively serving requests at a
time.  This is coordinated by performing leader election amongst the running instances through
ZooKeeper.  The active transaction server instance will also register itself using a service
discovery interface in ZooKeeper, allowing clients to discover the currently active server
instance without additional configuration.

Transaction Client
..................

A client makes a call to the active transaction server in order to start a new transaction.  This
returns a new transaction instance to the client, with a unique transaction ID (used to identify
writes for the transaction), as well as a list of transaction IDs to exclude for reads (from
in-progress or invalidated transactions).  When performing writes, the client overrides the
timestamp for all modified HBase cells with the transaction ID.  When reading data from HBase, the
client skips cells associated with any of the excluded transaction IDs.  The read exclusions are
applied through a server-side filter injected by the ``TransactionProcessor`` coprocessor.

TransactionProcessor Coprocessor
..................................

The ``TransactionProcessor`` coprocessor is loaded on all HBase tables where transactional reads
and writes are performed.  When clients read data, it coordinates the server-side filtering
performed based on the client transaction's snapshot. Data cells from any transactions that are
currently in-progress or those that have failed and could not be rolled back ("invalid" 
transactions) will be skipped on these reads.  In addition, the ``TransactionProcessor`` cleans 
up any data versions that are no longer visible to any running transactions, either because the 
transaction that the cell is associated with failed or a write from a newer transaction was 
successfully committed to the same column.

More details on how Tephra transactions work and the interactions between these components can be
found in our `Transactions over HBase
<http://www.slideshare.net/alexbaranau/transactions-over-hbase>`_ presentation.


Is It Building?
----------------
Status of continuous integration build at `Travis CI <https://travis-ci.org/caskdata/tephra>`__: |(BuildStatus)|

Requirements
------------

Java Runtime
............

The latest `JDK or JRE version 1.7.xx or 1.8.xx <http://www.java.com/en/download/manual.jsp>`__
for Linux, Windows, or Mac OS X must be installed in your environment; we recommend the Oracle JDK.

To check the Java version installed, run the command::

  $ java -version

Tephra is tested with the Oracle JDKs; it may work with other JDKs such as
`Open JDK <http://openjdk.java.net>`__, but it has not been tested with them.

Once you have installed the JDK, you'll need to set the JAVA_HOME environment variable.


Hadoop/HBase Environment
........................

Tephra requires a working HBase and HDFS environment in order to operate. Tephra supports these
component versions:

+---------------+-------------------+-----------------------------------------------------+
| Component     | Source            | Supported Versions                                  |
+===============+===================+=====================================================+
| **HDFS**      | Apache Hadoop     | 2.0.2-alpha through 2.6.0                           |
+               +-------------------+-----------------------------------------------------+
|               | CDH or HDP        | (CDH) 5.0.0 through 5.4.4 or (HDP) 2.0, 2.1, or 2.2 |
+               +-------------------+-----------------------------------------------------+
|               | MapR              | 4.1 (with MapR-FS)                                  |
+---------------+-------------------+-----------------------------------------------------+
| **HBase**     | Apache            | 0.96.x, 0.98.x, and 1.0.x                           |
+               +-------------------+-----------------------------------------------------+
|               | CDH or HDP        | (CDH) 5.0.0 through 5.4.4 or (HDP) 2.0, 2.1, or 2.2 |
+               +-------------------+-----------------------------------------------------+
|               | MapR              | 4.1 (with Apache HBase)                             |
+---------------+-------------------+-----------------------------------------------------+
| **Zookeeper** | Apache            | Version 3.4.3 through 3.4.5                         |
+               +-------------------+-----------------------------------------------------+
|               | CDH or HDP        | (CDH) 5.0.0 through 5.4.4 or (HDP) 2.0, 2.1, or 2.2 |
+               +-------------------+-----------------------------------------------------+
|               | MapR              | 4.1                                                 |
+---------------+-------------------+-----------------------------------------------------+

**Note:** Components versions shown in this table are those that we have tested and are
confident of their suitability and compatibility. Later versions of components may work,
but have not necessarily have been either tested or confirmed compatible.


Getting Started
---------------

You can get started with Tephra by building directly from the latest source code::

  git clone https://github.com/caskdata/tephra.git
  cd tephra
  mvn clean package

After the build completes, you will have a full binary distribution of Tephra under the
``tephra-distribution/target/`` directory.  Take the ``tephra-<version>.tar.gz`` file and install
it on your systems.

For any client applications, add the following dependencies to any Apache Maven POM files (or your
build system's equivalent configuration), in order to make use of Tephra classes::

  <dependency>
    <groupId>co.cask.tephra</groupId>
    <artifactId>tephra-api</artifactId>
    <version>0.5.0</version>
  </dependency>
  <dependency>
    <groupId>co.cask.tephra</groupId>
    <artifactId>tephra-core</artifactId>
    <version>0.5.0</version>
  </dependency>

Since the HBase APIs have changed between versions, you will need to select the
appropriate HBase compatibility library.

For HBase 0.96.x::

  <dependency>
    <groupId>co.cask.tephra</groupId>
    <artifactId>tephra-hbase-compat-0.96</artifactId>
    <version>0.5.0</version>
  </dependency>

For HBase 0.98.x::

  <dependency>
    <groupId>co.cask.tephra</groupId>
    <artifactId>tephra-hbase-compat-0.98</artifactId>
    <version>0.5.0</version>
  </dependency>

For HBase 1.0.x::

  <dependency>
    <groupId>co.cask.tephra</groupId>
    <artifactId>tephra-hbase-compat-1.0</artifactId>
    <version>0.5.0</version>
  </dependency>

If you are running the CDH 5.4 version of HBase 1.0.x (this version contains API incompatibilities
with Apache HBase 1.0.x)::

  <dependency>
    <groupId>co.cask.tephra</groupId>
    <artifactId>tephra-hbase-compat-1.0-cdh</artifactId>
    <version>0.5.0</version>
  </dependency>


Deployment and Configuration
----------------------------

Tephra makes use of a central transaction server to assign unique transaction IDs for data
modifications and to perform conflict detection.  Only a single transaction server can actively
handle client requests at a time, however, additional transaction server instances can be run
simultaneously, providing automatic failover if the active server becomes unreachable.

Transaction Server Configuration
................................

The Tephra transaction server can be deployed on the same cluster nodes running the HBase HMaster
process. The transaction server requires that the HBase libraries be available on the server's 
Java ``CLASSPATH``.  

The transaction server supports the following configuration properties.  All configuration
properties can be added to the ``hbase-site.xml`` file on the server's ``CLASSPATH``:

+-------------------------------+------------+-----------------------------------------------------------------+
| Name                          | Default    | Description                                                     |
+===============================+============+=================================================================+
| ``data.tx.bind.port``         | 15165      | Port to bind to                                                 |
+-------------------------------+------------+-----------------------------------------------------------------+
| ``data.tx.bind.address``      | 0.0.0.0    | Server address to listen on                                     |
+-------------------------------+------------+-----------------------------------------------------------------+
| ``data.tx.server.io.threads`` | 2          | Number of threads for socket IO                                 |
+-------------------------------+------------+-----------------------------------------------------------------+
| ``data.tx.server.threads``    | 20         | Number of handler threads                                       |
+-------------------------------+------------+-----------------------------------------------------------------+
| ``data.tx.timeout``           | 30         | Timeout for a transaction to complete (seconds)                 |
+-------------------------------+------------+-----------------------------------------------------------------+
| ``data.tx.long.timeout``      | 86400      | Timeout for a long running transaction to complete (seconds)    |
+-------------------------------+------------+-----------------------------------------------------------------+
| ``data.tx.cleanup.interval``  | 10         | Frequency to check for timed out transactions (seconds)         |
+-------------------------------+------------+-----------------------------------------------------------------+
| ``data.tx.snapshot.dir``      |            | HDFS directory used to store snapshots of tx state              |
+-------------------------------+------------+-----------------------------------------------------------------+
| ``data.tx.snapshot.interval`` | 300        | Frequency to write new snapshots                                |
+-------------------------------+------------+-----------------------------------------------------------------+
| ``data.tx.snapshot.retain``   | 10         | Number of old transaction snapshots to retain                   |
+-------------------------------+------------+-----------------------------------------------------------------+
| ``data.tx.metrics.period``    | 60         | Frequency for metrics reporting (seconds)                       |
+-------------------------------+------------+-----------------------------------------------------------------+

To run the Transaction server, execute the following command in your Tephra installation::

  ./bin/tephra start

Any environment-specific customizations can be made by editing the ``bin/tephra-env.sh`` script.


Client Configuration
....................

Since Tephra clients will be communicating with HBase, the HBase client libraries and the HBase cluster
configuration must be available on the client's Java ``CLASSPATH``.

Client API usage is described in the `Client APIs`_ section.

The transaction service client supports the following configuration properties.  All configuration
properties can be added to the ``hbase-site.xml`` file on the client's ``CLASSPATH``:

+------------------------------------------+-----------+-----------------------------------------------+
| Name                                     | Default   | Description                                   |
+==========================================+===========+===============================================+
| ``data.tx.client.timeout``               | 30000     | Client socket timeout (milliseconds)          |
+------------------------------------------+-----------+-----------------------------------------------+
| ``data.tx.client.provider``              | pool      | Client provider strategy: "pool" uses a pool  |
|                                          |           | of clients; "thread-local" a client per       |
|                                          |           | thread                                        |
+------------------------------------------+-----------+-----------------------------------------------+
| ``data.tx.client.count``                 | 5         | Max number of clients for "pool" provider     |
+------------------------------------------+-----------+-----------------------------------------------+
| ``data.tx.client.retry.strategy``        | backoff   | Client retry strategy: "backoff" for back off |
|                                          |           | between attempts; "n-times" for fixed number  |
|                                          |           | of tries                                      |
+------------------------------------------+-----------+-----------------------------------------------+
| ``data.tx.client.retry.attempts``        | 2         | Number of times to retry ("n-times" strategy) |
+------------------------------------------+-----------+-----------------------------------------------+
| ``data.tx.client.retry.backoff.initial`` | 100       | Initial sleep time ("backoff" strategy)       |
+------------------------------------------+-----------+-----------------------------------------------+
| ``data.tx.client.retry.backoff.factor``  | 4         | Multiplication factor for sleep time          |
+------------------------------------------+-----------+-----------------------------------------------+
| ``data.tx.client.retry.backoff.limit``   | 30000     | Exit when sleep time reaches this limit       |
+------------------------------------------+-----------+-----------------------------------------------+


HBase Coprocessor Configuration
...............................

In addition to the transaction server, Tephra requires an HBase coprocessor to be installed on all
tables where transactional reads and writes will be performed.  

To configure the coprocessor on all HBase tables, add the following to ``hbase-site.xml``.

For HBase 0.96::

  <property>
    <name>hbase.coprocessor.region.classes</name>
    <value>co.cask.tephra.hbase96.coprocessor.TransactionProcessor</value>
  </property>

For HBase 0.98::

  <property>
    <name>hbase.coprocessor.region.classes</name>
    <value>co.cask.tephra.hbase98.coprocessor.TransactionProcessor</value>
  </property>

For HBase 1.0::

  <property>
    <name>hbase.coprocessor.region.classes</name>
    <value>co.cask.tephra.hbase10.coprocessor.TransactionProcessor</value>
  </property>

For the CDH 5.4 version of HBase 1.0::

  <property>
    <name>hbase.coprocessor.region.classes</name>
    <value>co.cask.tephra.hbase10cdh.coprocessor.TransactionProcessor</value>
  </property>

You may configure the ``TransactionProcessor`` to be loaded only on HBase tables that you will
be using for transaction reads and writes.  However, you must ensure that the coprocessor is 
available on all impacted tables in order for Tephra to function correctly.

Metrics Reporting
.................

Tephra ships with built-in support for reporting metrics via JMX and a log file, using the
`Dropwizard Metrics <http://metrics.dropwizard.io>`_ library.

To enable JMX reporting for metrics, you will need to enable JMX in the Java runtime
arguments. Edit the ``bin/tephra-env.sh`` script and uncomment the following lines, making any
desired changes to configuration for port used, SSL, and JMX authentication::

  # export JMX_OPTS="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=13001"
  # export OPTS="$OPTS $JMX_OPTS"

To enable file-based reporting for metrics, edit the ``conf/logback.xml`` file and uncomment the
following section, replacing the ``FILE-PATH`` placeholder with a valid directory on the local
filesystem::

  <appender name="METRICS" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>/FILE-PATH/metrics.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
      <fileNamePattern>metrics.log.%d{yyyy-MM-dd}</fileNamePattern>
      <maxHistory>30</maxHistory>
    </rollingPolicy>
    <encoder>
      <pattern>%d{ISO8601} %msg%n</pattern>
    </encoder>
  </appender>
  <logger name="tephra-metrics" level="TRACE" additivity="false">
    <appender-ref ref="METRICS" />
  </logger>

The frequency of metrics reporting may be configured by setting the ``data.tx.metrics.period``
configuration property to the report frequency in seconds.


Client APIs
-----------
The ``TransactionAwareHTable`` class implements HBase's ``HTableInterface``, thus providing the same APIs
that a standard HBase ``HTable`` instance provides. Only certain operations are supported
transactionally. These are: 

.. csv-table::
  :header: Methods Supported In Transactions
  :widths: 100
  :delim: 0x9

    ``exists(Get get)``
    ``exists(List<Get> gets)``
    ``get(Get get)``
    ``get(List<Get> gets)``
    ``batch(List<? extends Row> actions, Object[] results)``
    ``batch(List<? extends Row> actions)``
    ``batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback)`` [0.96]
    ``batchCallback(List<? extends Row> actions, Batch.Callback<R> callback)`` [0.96]
    ``getScanner(byte[] family)``
    ``getScanner(byte[] family, byte[] qualifier)``
    ``put(Put put)``
    ``put(List<Put> puts)``
    ``delete(Delete delete)``
    ``delete(List<Delete> deletes)``

Other operations are not supported transactionally and will throw an ``UnsupportedOperationException`` if invoked.
To allow use of these non-transactional operations, call ``setAllowNonTransactional(true)``. This
allows you to call the following methods non-transactionally:

.. csv-table::
  :header: Methods Supported Outside of Transactions
  :widths: 100
  :delim: 0x9

    ``getRowOrBefore(byte[] row, byte[], family)``
    ``checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)``
    ``checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)``
    ``mutateRow(RowMutations rm)``
    ``append(Append append)``
    ``increment(Increment increment)``
    ``incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)``
    ``incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)``
    ``incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)``

Note that for ``batch`` operations, only certain supported operations (``get``, ``put``, and ``delete``)
are applied transactionally.

Usage
.....
To use a ``TransactionalAwareHTable``, you need an instance of ``TransactionContext``.
``TransactionContext`` provides the basic contract for client use of transactions.  At each point
in the transaction lifecycle, it provides the necessary interactions with the Tephra Transaction
Server in order to start, commit, and rollback transactions.  Basic usage of
``TransactionContext`` is handled using the following pattern:

.. code:: java

  TransactionContext context = new TransactionContext(client, transactionAwareHTable);
  try {
    context.start();
    transactionAwareHTable.put(new Put(Bytes.toBytes("row"));
    // ...
    context.finish();
  } catch (TransactionFailureException e) {
    context.abort();
  }

#. First, a new transaction is started using ``TransactionContext.start()``.
#. Next, any data operations are performed within the context of the transaction.
#. After data operations are complete, ``TransactionContext.finish()`` is called to commit the
   transaction.
#. If an exception occurs, ``TransactionContext.abort()`` can be called to rollback the
   transaction.

``TransactionAwareHTable`` handles the details of performing data operations transactionally, and
implements the necessary hooks in order to commit and rollback the data changes (see
``TransactionAware``).

Example
.......
To demonstrate how you might use ``TransactionAwareHTable``\s, below is a basic implementation of a
``SecondaryIndexTable``. This class encapsulates the usage of a ``TransactionContext`` and provides a simple interface
to a user:

.. code:: java

  /**
   * A Transactional SecondaryIndexTable.
   */
  public class SecondaryIndexTable {
    private byte[] secondaryIndex;
    private TransactionAwareHTable transactionAwareHTable;
    private TransactionAwareHTable secondaryIndexTable;
    private TransactionContext transactionContext;
    private final TableName secondaryIndexTableName;
    private static final byte[] secondaryIndexFamily = 
      Bytes.toBytes("secondaryIndexFamily");
    private static final byte[] secondaryIndexQualifier = Bytes.toBytes('r');
    private static final byte[] DELIMITER  = new byte[] {0};

    public SecondaryIndexTable(TransactionServiceClient transactionServiceClient, 
                               HTable hTable, byte[] secondaryIndex) {
      secondaryIndexTableName = 
            TableName.valueOf(hTable.getName().getNameAsString() + ".idx");
      HTable secondaryIndexHTable = null;
      HBaseAdmin hBaseAdmin = null;
      try {
        hBaseAdmin = new HBaseAdmin(hTable.getConfiguration());
        if (!hBaseAdmin.tableExists(secondaryIndexTableName)) {
          hBaseAdmin.createTable(new HTableDescriptor(secondaryIndexTableName));
        }
        secondaryIndexHTable = new HTable(hTable.getConfiguration(), 
                                          secondaryIndexTableName);
      } catch (Exception e) {
        Throwables.propagate(e);
      } finally {
        try {
          hBaseAdmin.close();
        } catch (Exception e) {
          Throwables.propagate(e);
        }
      }

      this.secondaryIndex = secondaryIndex;
      this.transactionAwareHTable = new TransactionAwareHTable(hTable);
      this.secondaryIndexTable = new TransactionAwareHTable(secondaryIndexHTable);
      this.transactionContext = new TransactionContext(transactionServiceClient, 
                                                       transactionAwareHTable,
                                                       secondaryIndexTable);
    }

    public Result get(Get get) throws IOException {
      return get(Collections.singletonList(get))[0];
    }

    public Result[] get(List<Get> gets) throws IOException {
      try {
        transactionContext.start();
        Result[] result = transactionAwareHTable.get(gets);
        transactionContext.finish();
        return result;
      } catch (Exception e) {
        try {
          transactionContext.abort();
        } catch (TransactionFailureException e1) {
          throw new IOException("Could not rollback transaction", e1);
        }
      }
      return null;
    }

    public Result[] getByIndex(byte[] value) throws IOException {
      try {
        transactionContext.start();
        Scan scan = new Scan(value, Bytes.add(value, new byte[0]));
        scan.addColumn(secondaryIndexFamily, secondaryIndexQualifier);
        ResultScanner indexScanner = secondaryIndexTable.getScanner(scan);

        ArrayList<Get> gets = new ArrayList<Get>();
        for (Result result : indexScanner) {
          for (Cell cell : result.listCells()) {
            gets.add(new Get(cell.getValue()));
          }
        }
        Result[] results = transactionAwareHTable.get(gets);
        transactionContext.finish();
        return results;
      } catch (Exception e) {
        try {
          transactionContext.abort();
        } catch (TransactionFailureException e1) {
          throw new IOException("Could not rollback transaction", e1);
        }
      }
      return null;
    }

    public void put(Put put) throws IOException {
      put(Collections.singletonList(put));
    }


    public void put(List<Put> puts) throws IOException {
      try {
        transactionContext.start();
        ArrayList<Put> secondaryIndexPuts = new ArrayList<Put>();
        for (Put put : puts) {
          List<Put> indexPuts = new ArrayList<Put>();
          Set<Map.Entry<byte[], List<KeyValue>>> familyMap = put.getFamilyMap().entrySet();
          for (Map.Entry<byte [], List<KeyValue>> family : familyMap) {
            for (KeyValue value : family.getValue()) {
              if (value.getQualifier().equals(secondaryIndex)) {
                byte[] secondaryRow = Bytes.add(value.getQualifier(),
                                                DELIMITER,
                                                Bytes.add(value.getValue(),
                                                DELIMITER,
                                                value.getRow()));
                Put indexPut = new Put(secondaryRow);
                indexPut.add(secondaryIndexFamily, secondaryIndexQualifier, put.getRow());
                indexPuts.add(indexPut);
              }
            }
          }
          secondaryIndexPuts.addAll(indexPuts);
        }
        transactionAwareHTable.put(puts);
        secondaryIndexTable.put(secondaryIndexPuts);
        transactionContext.finish();
      } catch (Exception e) {
        try {
          transactionContext.abort();
        } catch (TransactionFailureException e1) {
          throw new IOException("Could not rollback transaction", e1);
        }
      }
    }
  }


Known Issues and Limitations
----------------------------

- Currently, column family ``Delete`` operations are implemented by writing a cell with an empty
  qualifier (empty ``byte[]``) and empty value (empty ``byte[]``).  This is done in place of
  native HBase ``Delete`` operations so the delete marker can be rolled back in the event of
  a transaction failure -- normal HBase ``Delete`` operations cannot be undone.  However, this
  means that applications that store data in a column with an empty qualifier will not be able to
  store empty values, and will not be able to transactionally delete that column.
- Column ``Delete`` operations are implemented by writing a empty value (empty ``byte[]``) to the
  column.  This means that applications will not be able to store empty values to columns.
- Invalid transactions are not automatically cleared from the exclusion list.  When a transaction is
  invalidated, either from timing out or being invalidated by the client due to a failure to rollback
  changes, its transaction ID is added to a list of excluded transactions.  Data from invalidated
  transactions will be dropped by the ``TransactionProcessor`` coprocessor on HBase region flush
  and compaction operations.  Currently, however, transaction IDs can only be manually removed
  from the list of excluded transaction IDs, using the ``co.cask.tephra.TransactionAdmin`` tool.


How to Contribute
-----------------

Interested in helping to improve Tephra? We welcome all contributions, whether in filing detailed
bug reports, submitting pull requests for code changes and improvements, or by asking questions and
assisting others on the mailing list.

Bug Reports & Feature Requests
..............................

Bugs and tasks are tracked in a public JIRA `issue tracker <https://issues.cask.co/browse/TEPHRA>`__.

Mailing List
............

Tephra User Group and Development Discussions: `tephra-dev@googlegroups.com 
<https://groups.google.com/d/forum/tephra-dev>`__

IRC
...

Have questions about how Tephra works, or need help using it?  Drop by the ``#tephra``
chat room on ``irc.freenode.net``.

Pull Requests
.............

We have a simple pull-based development model with a consensus-building phase, similar to Apache's
voting process. If you’d like to help make Tephra better by adding new features, enhancing existing
features, or fixing bugs, here's how to do it:

#. If you are planning a large change or contribution, discuss your plans on the ``tephra-dev``
   mailing list first.  This will help us understand your needs and best guide your solution in a
   way that fits the project.
#. Fork Tephra into your own GitHub repository.
#. Create a topic branch with an appropriate name.
#. Work on the code to your heart's content.
#. Once you’re satisfied, create a pull request from your GitHub repo (it’s helpful if you fill in
   all of the description fields).
#. After we review and accept your request, we’ll commit your code to the caskdata/tephra
   repository.

Thanks for helping to improve Tephra!


License and Trademarks
----------------------

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License
is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied. See the License for the specific language governing permissions and limitations under
the License.

Cask, Cask Tephra and Tephra are trademarks of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with permission. 
No endorsement by The Apache Software Foundation is implied by the use of these marks.

.. |(TM)| unicode:: U+2122 .. trademark sign
   :trim:

.. |(Tephra)| image:: docs/_images/tephra_logo_light_bknd_cask.png

.. |(BuildStatus)| image:: https://travis-ci.org/caskdata/tephra.svg?branch=develop
   :target: https://travis-ci.org/caskdata/tephra
