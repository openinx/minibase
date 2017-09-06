package org.apache.minibase;

import jdk.nashorn.internal.ir.Block;
import org.apache.minibase.DiskFile.BlockReader;
import org.apache.minibase.DiskFile.BlockWriter;
import org.apache.minibase.DiskFile.DiskFileWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestDiskFile {

  @Test
  public void testBlockEncoding() throws IOException {
    BlockWriter bw = new BlockWriter();
    for (int i = 0; i < 100; i++) {
      bw.append(new KeyValue(Bytes.toBytes(i), Bytes.toBytes(i)));
    }
    Assert.assertEquals(bw.getLastKV(), new KeyValue(Bytes.toBytes(99), Bytes.toBytes(99)));

    byte[] buffer = bw.serialize();
    BlockReader br = BlockReader.parseFrom(bw.serialize(), 0, buffer.length);

    // Assert the bloom filter.
    byte[][] bytes = new byte[br.getKeyValues().size()][];
    for (int i = 0; i < br.getKeyValues().size(); i++) {
      bytes[i] = br.getKeyValues().get(i).getKey();
    }
    BloomFilter bloom =
        new BloomFilter(DiskFile.BLOOM_FILTER_HASH_COUNT, DiskFile.BLOOM_FILTER_BITS_PER_KEY);
    Assert.assertArrayEquals(bloom.generate(bytes), bw.getBloomFilter());
  }

  @Test
  public void testDiskFileWriter() throws IOException {
    DiskFileWriter diskWriter = new DiskFileWriter("/Users/openinx/minibase.db");

    for (int i = 0; i < 100000000; i++) {
      diskWriter.append(KeyValue.create(Bytes.toBytes(i), Bytes.toBytes(i)));
    }

    diskWriter.appendIndex();
    diskWriter.appendTrailer();
    diskWriter.close();
  }
}
