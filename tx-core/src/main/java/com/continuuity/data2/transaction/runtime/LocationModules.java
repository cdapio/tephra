/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction.runtime;

import com.continuuity.data2.transaction.TxConstants;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Provides access to Google Guice modules for in-memory, single-node, and distributed operation for
 * {@link LocationFactory}.
 */
public final class LocationModules {

  private static final Logger LOG = LoggerFactory.getLogger(LocationModules.class);

  public Module getInMemoryModules() {
    return new LocalLocationModule();
  }

  public Module getSingleNodeModules() {
    return new LocalLocationModule();
  }

  public Module getDistributedModules() {
    return new HDFSLocationModule();
  }

  private static final class LocalLocationModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(LocationFactory.class).to(LocalLocationFactory.class);
    }

    @Provides
    @Singleton
    private LocalLocationFactory providesLocalLocationFactory(Configuration conf) {
      return new LocalLocationFactory(new File(conf.get(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR)));
    }
  }

  private static final class HDFSLocationModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(LocationFactory.class).to(HDFSLocationFactory.class);
    }

    @Provides
    @Singleton
    private HDFSLocationFactory providesHDFSLocationFactory(Configuration conf) {
      String hdfsUser = conf.get(TxConstants.Manager.CFG_TX_HDFS_USER);
      FileSystem fileSystem;

      try {
        if (hdfsUser == null || UserGroupInformation.isSecurityEnabled()) {
          if (hdfsUser != null && LOG.isDebugEnabled()) {
            LOG.debug("Ignoring configuration {}={}, running on secure Hadoop",
                      TxConstants.Manager.CFG_TX_HDFS_USER, hdfsUser);
          }
          fileSystem = FileSystem.get(FileSystem.getDefaultUri(conf), conf);
        } else {
          fileSystem = FileSystem.get(FileSystem.getDefaultUri(conf), conf, hdfsUser);
        }
        return new HDFSLocationFactory(fileSystem);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
