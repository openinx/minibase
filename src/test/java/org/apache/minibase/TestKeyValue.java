package org.apache.minibase;

import org.junit.Assert;
import org.junit.Test;

public class TestKeyValue {

  @Test
  public void testCompare() {
    KeyValue kv = new KeyValue(Bytes.toBytes(100), Bytes.toBytes(1000));
    Assert.assertFalse(kv.equals(null));
    Assert.assertFalse(kv.equals(new Object()));
    Assert.assertTrue(kv.equals(new KeyValue(Bytes.toBytes(100), Bytes.toBytes(1000))));
    Assert.assertFalse(kv.equals(new KeyValue(Bytes.toBytes(100L), Bytes.toBytes(1000))));
    Assert.assertTrue(kv.equals(new KeyValue(Bytes.toBytes(100), Bytes.toBytes(1000L))));
  }
}
