package org.apache.minibase;

import org.apache.minibase.DiskFile.BlockMeta;
import org.apache.minibase.DiskFile.BlockReader;
import org.apache.minibase.DiskFile.BlockWriter;
import org.apache.minibase.DiskFile.DiskFileWriter;
import org.apache.minibase.MiniBase.Iter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class TestDiskFile {

  public static final Random RANDOM = new Random();

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
  public void testBlockMeta() throws IOException {
    byte[] lastKey = Bytes.toBytes("abc");
    long offset = 1024, size = 1024;
    byte[] bloomFilter = Bytes.toBytes("bloomFilter");

    BlockMeta meta = new BlockMeta(lastKey, offset, size, bloomFilter);
    byte[] buffer = meta.toBytes();

    BlockMeta meta2 = BlockMeta.parseFrom(buffer, 0);

    Assert.assertArrayEquals(lastKey, meta2.getKey());
    Assert.assertEquals(offset, meta2.getOffset());
    Assert.assertEquals(size, meta2.getSize());
    Assert.assertArrayEquals(bloomFilter, meta2.getBloomFilter());
  }

  private byte[] generateRandomBytes() {
    int len = (RANDOM.nextInt() % 1024 + 1024) % 1024;
    byte[] buffer = new byte[len];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) (RANDOM.nextInt() & 0xFF);
    }
    return buffer;
  }

  @Test
  public void testDiskFile() throws IOException {
    String dbFile = "testDiskFileWriter.db";
    try {
      try (DiskFileWriter diskWriter = new DiskFileWriter(dbFile)) {
        for (int i = 0; i < 1000000; i++) {
          diskWriter.append(KeyValue.create(generateRandomBytes(), generateRandomBytes()));
        }
        diskWriter.appendIndex();
        diskWriter.appendTrailer();
      }
      try (DiskFile df = new DiskFile()) {
        df.open(dbFile);
      }
    } finally {
      // Remove the dbFile.
      File f = new File(dbFile);
      if (f.exists()) {
        f.delete();
      }
    }
  }

  @Test
  public void testDiskFileIO() throws IOException {
    String dbFile = "testDiskFileIO.db";
    int rowsCount = 1000000;

    try {
      DiskFileWriter diskWriter = new DiskFileWriter(dbFile);

      for (int i = 0; i < rowsCount; i++) {
        diskWriter.append(KeyValue.create(Bytes.toBytes(i), Bytes.toBytes(i)));
      }

      diskWriter.appendIndex();
      diskWriter.appendTrailer();
      diskWriter.close();

      try (DiskFile df = new DiskFile()) {
        df.open(dbFile);
        Iter<KeyValue> it = df.iterator();
        int index = 0;
        while (it.hasNext()) {
          KeyValue kv = it.next();
          Assert.assertEquals(KeyValue.create(Bytes.toBytes(index), Bytes.toBytes(index)), kv);
          index++;
        }
        Assert.assertEquals(index, rowsCount);
      }
    } finally {
      // Remove the dbFile.
      File f = new File(dbFile);
      if (f.exists()) {
        f.delete();
      }
    }
  }
}
