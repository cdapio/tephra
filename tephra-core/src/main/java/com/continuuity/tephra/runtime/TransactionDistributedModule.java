/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.tephra.runtime;

import com.continuuity.tephra.DefaultTransactionExecutor;
import com.continuuity.tephra.TransactionExecutor;
import com.continuuity.tephra.TransactionExecutorFactory;
import com.continuuity.tephra.TransactionSystemClient;
import com.continuuity.tephra.distributed.TransactionServiceClient;
import com.continuuity.tephra.inmemory.InMemoryTransactionManager;
import com.continuuity.tephra.persist.HDFSTransactionStateStorage;
import com.continuuity.tephra.persist.TransactionStateStorage;
import com.continuuity.tephra.snapshot.SnapshotCodecProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

/**
 * Guice bindings for running in distributed mode on a cluster.
 */
final class TransactionDistributedModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SnapshotCodecProvider.class).in(Singleton.class);
    bind(TransactionStateStorage.class).annotatedWith(Names.named("persist"))
      .to(HDFSTransactionStateStorage.class).in(Singleton.class);
    bind(TransactionStateStorage.class).toProvider(TransactionStateStorageProvider.class).in(Singleton.class);

    bind(InMemoryTransactionManager.class).in(Singleton.class);
    bind(TransactionSystemClient.class).to(TransactionServiceClient.class).in(Singleton.class);

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));
  }
}
