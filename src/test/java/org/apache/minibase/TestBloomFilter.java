package org.apache.minibase;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestBloomFilter {

  @Test
  public void testBloomFilter() throws IOException {
    String[] keys = { "hello world", "hi", "bloom", "filter", "key", "value", "1", "value" };
    BloomFilter bf = new BloomFilter(3, 10);
    byte[][] keyBytes = new byte[keys.length][];
    for (int i = 0; i < keys.length; i++) {
      keyBytes[i] = keys[i].getBytes();
    }
    bf.generate(keyBytes);
    Assert.assertTrue(bf.contains(Bytes.toBytes("hi")));
    Assert.assertFalse(bf.contains(Bytes.toBytes("h")));
    Assert.assertFalse(bf.contains(Bytes.toBytes("he")));
    Assert.assertTrue(bf.contains(Bytes.toBytes("hello world")));
    Assert.assertTrue(bf.contains(Bytes.toBytes("bloom")));
    Assert.assertTrue(bf.contains(Bytes.toBytes("key")));
  }
}
