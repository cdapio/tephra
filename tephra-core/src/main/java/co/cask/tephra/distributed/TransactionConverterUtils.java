/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.tephra.distributed;

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionType;
import co.cask.tephra.distributed.thrift.TTransaction;
import co.cask.tephra.distributed.thrift.TTransactionType;
import com.google.common.primitives.Longs;

/**
 * Utility methods to convert to thrift and back.
 */
public final class TransactionConverterUtils {

  public static TTransaction wrap(Transaction tx) {
    return new TTransaction(tx.getTransactionId(), tx.getReadPointer(),
                            Longs.asList(tx.getInvalids()), Longs.asList(tx.getInProgress()),
                            tx.getFirstShortInProgress(), getTTransactionType(tx.getType()),
                            tx.getWritePointer(), Longs.asList(tx.getCheckpointWritePointers()));
  }

  public static Transaction unwrap(TTransaction thriftTx) {
    return new Transaction(thriftTx.getReadPointer(), thriftTx.getTransactionId(), thriftTx.getWritePointer(),
                           Longs.toArray(thriftTx.getInvalids()), Longs.toArray(thriftTx.getInProgress()),
                           thriftTx.getFirstShort(), getTransactionType(thriftTx.getType()),
                           Longs.toArray(thriftTx.getCheckpointWritePointers()));
  }

  private static TransactionType getTransactionType(TTransactionType tType) {
    return tType == TTransactionType.SHORT ? TransactionType.SHORT : TransactionType.LONG;
  }

  private static TTransactionType getTTransactionType(TransactionType type) {
    return type == TransactionType.SHORT ? TTransactionType.SHORT : TTransactionType.LONG;
  }
}
