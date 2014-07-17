==================
Continuuity Tephra
==================

-----------------------------
Transactions for Apache HBase
-----------------------------

Continuuity Tephra provides globally consistent transactions on top of Apache HBase.  While HBase
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
- **TransactionDataJanitor Coprocessor** - applies filtering to the data read (based on a 
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
applied through a server-side filter injected by the ``TransactionDataJanitor`` coprocessor.

TransactionDataJanitor Coprocessor
..................................

The ``TransactionDataJanitor`` coprocessor is loaded on all HBase tables where transactional reads
and writes are performed.  When clients read data, it coordinates the server-side filtering
performed based on the client transaction's snapshot. Data cells from any transactions that are
currently in-progress or those that have failed and could not be rolled back ("invalid" 
transactions) will be skipped on these reads.  In addition, the ``TransactionDataJanitor`` cleans 
up any data versions that are no longer visible to any running transactions, either because the 
transaction that the cell is associated with failed or a write from a newer transaction was 
successfully committed to the same column.

More details on how Tephra transactions work and the interactions between these components can be
found in our `Transactions over HBase
<http://www.slideshare.net/alexbaranau/transactions-over-hbase>`_ presentation.


Getting Started
---------------

You can get started with Tephra by building directly from the latest source code::

  git clone https://github.com/continuuity/tephra.git
  cd tephra
  mvn clean install assembly:single

After the build completes, add the following dependencies to any Apache Maven projects, in
order to make use of Tephra classes::

  <dependency>
    <groupId>com.continuuity</groupId>
    <artifactId>tephra-api</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </dependency>
  <dependency>
    <groupId>com.continuuity</groupId>
    <artifactId>tephra-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </dependency>

Since the HBase APIs have changed between version 0.94.x and 0.96.x, you will need to select the
appropriate HBase compatibility library.

For HBase 0.94.x::

  <dependency>
    <groupId>com.continuuity</groupId>
    <artifactId>tephra-hbase-compat-0.94</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </dependency>

For HBase 0.96.x and 0.98.x::

  <dependency>
    <groupId>com.continuuity</groupId>
    <artifactId>tephra-hbase-compat-0.96</artifactId>
    <version>0.1.0-SNAPSHOT</version>
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

+---------------------------+------------+---------------------------------------------------------+
| Name                      | Default    | Description                                             |
+===========================+============+=========================================================+
| data.tx.bind.port         | 15165      | Port to bind to                                         |
+---------------------------+------------+---------------------------------------------------------+
| data.tx.bind.address      | 0.0.0.0    | Server address to listen on                             |
+---------------------------+------------+---------------------------------------------------------+
| data.tx.server.io.threads | 2          | Number of threads for socket IO                         |
+---------------------------+------------+---------------------------------------------------------+
| data.tx.server.threads    | 20         | Number of handler threads                               |
+---------------------------+------------+---------------------------------------------------------+
| data.tx.timeout           | 30         | Timeout for a transaction to complete (seconds)         |
+---------------------------+------------+---------------------------------------------------------+
| data.tx.cleanup.interval  | 10         | Frequency to check for timed out transactions (seconds) |  
+---------------------------+------------+---------------------------------------------------------+
| data.tx.snapshot.dir      |            | HDFS directory used to store snapshots of tx state      |
+---------------------------+------------+---------------------------------------------------------+
| data.tx.snapshot.interval | 300        | Frequency to write new snapshots                        |
+---------------------------+------------+---------------------------------------------------------+
| data.tx.snapshot.retain   | 10         | Number of old transaction snapshots to retain           |
+---------------------------+------------+---------------------------------------------------------+

TODO: Add details on running transaction server when ENG-4084 is merged

Client Configuration
....................

Since Tephra clients will be communicating with HBase, the HBase client libraries and the HBase cluster
configuration must be available on the client's Java ``CLASSPATH``.

TODO: add link to client API usage once example guide is merged in

The transaction service client supports the following configuration properties.  All configuration
properties can be added to the ``hbase-site.xml`` file on the client's ``CLASSPATH``:

+--------------------------------------+-----------+-----------------------------------------------+
| Name                                 | Default   | Description                                   |
+======================================+===========+===============================================+
| data.tx.client.timeout               | 30000     | Client socket timeout (milliseconds)          |
+--------------------------------------+-----------+-----------------------------------------------+
| data.tx.client.provider              | pool      | Client provider strategy: "pool" uses a pool  |
|                                      |           | of clients; "thread-local" a client per       |
|                                      |           | thread                                        |
+--------------------------------------+-----------+-----------------------------------------------+
| data.tx.client.count                 | 5         | Max number of clients for "pool" provider     |
+--------------------------------------+-----------+-----------------------------------------------+
| data.tx.client.retry.strategy        | backoff   | Client retry strategy: "backoff" for back off |
|                                      |           | between attempts; "n-times" for fixed number  |
|                                      |           | of tries                                      |
+--------------------------------------+-----------+-----------------------------------------------+
| data.tx.client.retry.attempts        | 2         | Number of times to retry ("n-times" strategy) |
+--------------------------------------+-----------+-----------------------------------------------+
| data.tx.client.retry.backoff.initial | 100       | Initial sleep time ("backoff" strategy)       |
+--------------------------------------+-----------+-----------------------------------------------+
| data.tx.client.retry.backoff.factor  | 4         | Multiplication factor for sleep time          |
+--------------------------------------+-----------+-----------------------------------------------+
| data.tx.client.retry.backoff.limit   | 30000     | Exit when sleep time reaches this limit       |
+--------------------------------------+-----------+-----------------------------------------------+


HBase Coprocessor Configuration
...............................

In addition to the transaction server, Tephra requires an HBase coprocessor to be installed on all
tables where transactional reads and writes will be performed.  

To configure the coprocessor on all HBase tables, add the following to ``hbase-site.xml``.

For HBase 0.94::

  <property>
    <name>hbase.coprocessor.region.classes</name>
    <value>com.continuuity.data2.transaction.coprocessor.hbase94.TransactionDataJanitor</value>
  </property>

For HBase 0.96 and 0.98::

  <property>
    <name>hbase.coprocessor.region.classes</name>
    <value>com.continuuity.data2.transaction.coprocessor.hbase96.TransactionDataJanitor</value>
  </property>


You may configure the ``TransactionDataJanitor`` to be only on HBase tables that you will
be using for transaction reads and writes.  However, you must ensure that the coprocessor is 
available on all impacted tables in order for Tephra to function correctly.


Known Issues and Limitations
----------------------------

- Currently, ``Delete`` operations are implemented by writing a empty value (empty ``byte[]``) to the
  column.  This is necessary so that the changes can be rolled back in the case of a transaction
  failure -- normal HBase ``Delete`` operations cannot be undone.
- Invalid transactions are not cleared from the exclusion list.  When a transaction is
  invalidated, either from timing out or being invalidated by the client due to a failure to rollback
  changes, its transaction ID is added to a list of excluded transactions.  Data from invalidated
  transactions will be dropped by the ``TransactionDataJanitor`` coprocessor on HBase region flush
  and compaction operations.  Currently, however, the transaction ID is not removed from the list
  of excluded transaction IDs.


How to Contribute
-----------------

Interested in helping to improve Tephra? We welcome all contributions, whether in filing detailed
bug reports, submitting pull requests for code changes and improvements, or by asking questions and
assisting others on the mailing list.

Bug Reports & Feature Requests
..............................

Bugs and tasks are tracked in a public JIRA issue tracker.  Details on access will be forthcoming.

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
#. After we review and accept your request, we’ll commit your code to the continuuity/tephra
   repository.

Thanks for helping to improve Tephra!

Mailing List
............

Tephra User Group and Development Discussions: tephra-dev@googlegroups.com


License
-------

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License
is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied. See the License for the specific language governing permissions and limitations under
the License.
