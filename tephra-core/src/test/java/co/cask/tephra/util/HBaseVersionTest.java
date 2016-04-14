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

package co.cask.tephra.util;

import org.junit.Test;

import java.text.ParseException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests for HBase version parsing.
 */
public class HBaseVersionTest {
  @Test
  public void testVersionNumber() throws Exception {
    HBaseVersion.VersionNumber ver = HBaseVersion.VersionNumber.create("1");
    assertVersionNumber(ver, 1, null, null, null, false);

    ver = HBaseVersion.VersionNumber.create("1-SNAPSHOT");
    assertVersionNumber(ver, 1, null, null, null, true);

    ver = HBaseVersion.VersionNumber.create("1-foo");
    assertVersionNumber(ver, 1, null, null, "foo", false);

    ver = HBaseVersion.VersionNumber.create("1-foo-SNAPSHOT");
    assertVersionNumber(ver, 1, null, null, "foo", true);

    ver = HBaseVersion.VersionNumber.create("10.0");
    assertVersionNumber(ver, 10, 0, null, null, false);

    ver = HBaseVersion.VersionNumber.create("10.0-bar");
    assertVersionNumber(ver, 10, 0, null, "bar", false);

    ver = HBaseVersion.VersionNumber.create("3.2.1");
    assertVersionNumber(ver, 3, 2, 1, null, false);

    ver = HBaseVersion.VersionNumber.create("3.2.1-SNAPSHOT");
    assertVersionNumber(ver, 3, 2, 1, null, true);

    ver = HBaseVersion.VersionNumber.create("3.2.1-baz");
    assertVersionNumber(ver, 3, 2, 1, "baz", false);

    ver = HBaseVersion.VersionNumber.create("3.2.1-baz1.2.3");
    assertVersionNumber(ver, 3, 2, 1, "baz1.2.3", false);

    ver = HBaseVersion.VersionNumber.create("3.2.1-baz1.2.3-SNAPSHOT");
    assertVersionNumber(ver, 3, 2, 1, "baz1.2.3", true);

    try {
      ver = HBaseVersion.VersionNumber.create("abc");
      fail("Invalid verison number 'abc' should have thrown a ParseException");
    } catch (ParseException pe) {
      // expected
    }

    try {
      ver = HBaseVersion.VersionNumber.create("1.a.b");
      fail("Invalid verison number '1.a.b' should have thrown a ParseException");
    } catch (ParseException pe) {
      // expected
    }

    ver = HBaseVersion.VersionNumber.create("1.2.0-CDH5.7.0");
    assertVersionNumber(ver, 1, 2, 0, "CDH5.7.0", false);
  }

  private void assertVersionNumber(HBaseVersion.VersionNumber version, Integer expectedMajor, Integer expectedMinor,
                                   Integer expectedPatch, String expectedClassifier, boolean snapshot) {
    if (expectedMajor == null) {
      assertNull(version.getMajor());
    } else {
      assertEquals(expectedMajor, version.getMajor());
    }
    if (expectedMinor == null) {
      assertNull(version.getMinor());
    } else {
      assertEquals(expectedMinor, version.getMinor());
    }
    if (expectedPatch == null) {
      assertNull(version.getPatch());
    } else {
      assertEquals(expectedPatch, version.getPatch());
    }
    if (expectedClassifier == null) {
      assertNull(version.getClassifier());
    } else {
      assertEquals(expectedClassifier, version.getClassifier());
    }
    assertEquals(snapshot, version.isSnapshot());
  }
}
