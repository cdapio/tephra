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

package co.cask.tephra.hbase11;

import co.cask.tephra.util.AbstractConfigurationProviderTest;
import co.cask.tephra.util.HBaseVersion;

/**
 * Test for HBase 1.1 version specific behavior.
 */
public class HBase11ConfigurationProviderTest extends AbstractConfigurationProviderTest {
  @Override
  protected HBaseVersion.Version getExpectedVersion() {
    return HBaseVersion.Version.HBASE_11;
  }
}
