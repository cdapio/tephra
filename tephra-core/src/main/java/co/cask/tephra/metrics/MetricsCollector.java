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

package co.cask.tephra.metrics;

import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public interface MetricsCollector extends Service {
  /**
   * Send metric value for a given metric, identified by the metricsName, can also be tagged
   */
  void gauge(String metricName, int value, String... tags);

  void rate(String metricName);

  void histogram(String metricName, int value);

  void configure(Configuration conf);
}
