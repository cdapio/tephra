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

/**
 * This package contains example applications for Tephra designed to illustrate sample Tephra usage
 * and provide out-of-the-box sample applications which can be run to test cluster functionality.
 *
 * <p>Currently the following applications are provided:
 *
 * <ul>
 *   <li><strong>BalanceBooks</strong> - this application runs a specified number of concurrent clients in separate
 *     threads, which perform transactions to make withdrawals from each other's accounts and deposits to their own
 *     accounts.  At the end of the test, the total value of all account balances is verified to be equal to zero,
 *     which confirms that transactional integrity was not violated.
 *   </li>
 * </ul>
 * </p>
 *
 * <p>
 *   Note that, for simplicity, the examples package is currently hardcoded to compile against a specific HBase
 *   version (currently 1.0-cdh).  In the future, we should provide Maven profiles to allow compiling the examples
 *   against each of the supported HBase versions.
 * </p>
 */
package co.cask.tephra.examples;
