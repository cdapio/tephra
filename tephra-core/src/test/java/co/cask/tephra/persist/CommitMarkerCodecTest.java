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

package co.cask.tephra.persist;

import co.cask.tephra.TxConstants;
import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Unit Test for {@link CommitMarkerCodec}.
 */
public class CommitMarkerCodecTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final String LOG_FILE = "txlog";
  private static final Random RANDOM = new Random();

  private static MiniDFSCluster dfsCluster;
  private static Configuration conf;
  private static FileSystem fs;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TMP_FOLDER.newFolder().getAbsolutePath());

    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    conf = new Configuration(dfsCluster.getFileSystem().getConf());
    fs = FileSystem.newInstance(FileSystem.getDefaultUri(conf), conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    dfsCluster.shutdown();
  }

  @Test
  public void testRandomCommitMarkers() throws Exception {
    List<Integer> randomInts = new ArrayList<>();
    Path newLog = new Path(TMP_FOLDER.newFolder().getAbsolutePath(), LOG_FILE);

    // Write a bunch of random commit markers
    try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, newLog, LongWritable.class,
                                                                LongWritable.class,
                                                                SequenceFile.CompressionType.NONE)) {
      for (int i = 0; i < 1000; i++) {
        int randomNum = RANDOM.nextInt(Integer.MAX_VALUE);
        CommitMarkerCodec.writeMarker(writer, randomNum);
        randomInts.add(randomNum);
      }
      writer.hflush();
      writer.hsync();
    }

    // Read the commit markers back to verify the marker
    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, newLog, conf);
         CommitMarkerCodec markerCodec = new CommitMarkerCodec()) {
      for (int num : randomInts) {
        Assert.assertEquals(num, markerCodec.readMarker(reader));
      }
    }
  }

  private static class IncompleteValueBytes implements SequenceFile.ValueBytes {

    @Override
    public void writeUncompressedBytes(DataOutputStream outStream) throws IOException {
      // don't write anything to simulate incomplete record
    }

    @Override
    public void writeCompressedBytes(DataOutputStream outStream) throws IllegalArgumentException, IOException {
      throw new IllegalArgumentException("Not possible");
    }

    @Override
    public int getSize() {
      return Ints.BYTES;
    }
  }

  @Test
  public void testIncompleteCommitMarker() throws Exception {
    Path newLog = new Path(TMP_FOLDER.newFolder().getAbsolutePath(), LOG_FILE);
    try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, newLog, LongWritable.class,
                                                                LongWritable.class,
                                                                SequenceFile.CompressionType.NONE)) {
      String key = TxConstants.TransactionLog.NUM_ENTRIES_APPENDED;
      SequenceFile.ValueBytes valueBytes = new IncompleteValueBytes();
      writer.appendRaw(key.getBytes(), 0, key.length(), valueBytes);
      writer.hflush();
      writer.hsync();
    }

    // Read the incomplete commit marker
    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, newLog, conf);
         CommitMarkerCodec markerCodec = new CommitMarkerCodec()) {
      try {
        markerCodec.readMarker(reader);
        Assert.fail("Expected EOF Exception to be thrown");
      } catch (EOFException e) {
        // expected since we didn't write the value bytes
      }
    }
  }

  @Test
  public void testIncorrectCommitMarker() throws Exception {
    Path newLog = new Path(TMP_FOLDER.newFolder().getAbsolutePath(), LOG_FILE);

    // Write an incorrect marker
    try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, newLog, LongWritable.class,
                                                                LongWritable.class,
                                                                SequenceFile.CompressionType.NONE)) {
      String invalidKey = "IncorrectKey";
      SequenceFile.ValueBytes valueBytes = new CommitMarkerCodec.CommitEntriesCount(100);
      writer.appendRaw(invalidKey.getBytes(), 0, invalidKey.length(), valueBytes);
      writer.hflush();
      writer.hsync();
    }

    // Read the commit markers back to verify the marker
    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, newLog, conf);
         CommitMarkerCodec markerCodec = new CommitMarkerCodec()) {
      try {
        markerCodec.readMarker(reader);
        Assert.fail("Expected an IOException to be thrown");
      } catch (IOException e) {
        // expected
      }
    }
  }
}
