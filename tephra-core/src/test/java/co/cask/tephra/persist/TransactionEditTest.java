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

package co.cask.tephra.persist;

import co.cask.tephra.ChangeId;
import com.google.common.collect.Sets;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.IOException;

/**
 * test for {@link TransactionEdit}
 */
public class TransactionEditTest {
  private static final byte[] COL = new byte[] {'c'};

  @Test
  public void testV1SerdeCompat() throws Exception {
    TransactionEdit.TransactionEditCodec olderCodec = new TransactionEdit.TransactionEditCodecV1();
    // start tx edit and committed tx edit cover all fields of tx edit
    // NOTE: set visibilityUpperBound to 0 and transaction type to null as this is expected default 
    // for decoding older versions that doesn't store it
    verifyDecodingSupportsOlderVersion(TransactionEdit.createStarted(2L, 0L, 1000L, null), olderCodec);
    verifyDecodingSupportsOlderVersion(
      TransactionEdit.createCommitted(2L, Sets.newHashSet(new ChangeId(COL)), 3L, true), olderCodec);
  }
  
  @Test
  public void testV2SerdeCompat() throws Exception {
    TransactionEdit.TransactionEditCodec olderCodec = new TransactionEdit.TransactionEditCodecV2();
    // start tx edit and committed tx edit cover all fields of tx edit
    // NOTE: transaction type to null as this is expected default for decoding older versions that doesn't store it
    verifyDecodingSupportsOlderVersion(TransactionEdit.createStarted(2L, 100L, 1000L, null), olderCodec);
    verifyDecodingSupportsOlderVersion(
      TransactionEdit.createCommitted(2L, Sets.newHashSet(new ChangeId(COL)), 3L, true), olderCodec);
  }

  @SuppressWarnings("deprecation")
  private void verifyDecodingSupportsOlderVersion(TransactionEdit edit, 
                                                  TransactionEdit.TransactionEditCodec olderCodec)
    throws IOException {
    // encoding with older version of codec
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    olderCodec.encode(edit, out);

    // decoding
    TransactionEdit decodedEdit = new TransactionEdit();
    DataInput in = ByteStreams.newDataInput(out.toByteArray());
    decodedEdit.readFields(in);

    Assert.assertEquals(edit, decodedEdit);
  }
}
