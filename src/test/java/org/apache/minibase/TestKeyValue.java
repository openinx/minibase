package org.apache.minibase;

import org.junit.Assert;
import org.junit.Test;

public class TestKeyValue {

  @Test
  public void testCompare() {
    KeyValue kv = KeyValue.createPut(Bytes.toBytes(100), Bytes.toBytes(1000), 0L);
    Assert.assertFalse(kv.equals(null));
    Assert.assertFalse(kv.equals(new Object()));
    Assert.assertTrue(kv.equals(KeyValue.createPut(Bytes.toBytes(100), Bytes.toBytes(1000), 0L
    )));
    Assert.assertFalse(kv.equals(KeyValue.createPut(Bytes.toBytes(100L), Bytes.toBytes(1000), 0L)));
    Assert.assertTrue(kv.equals(KeyValue.createPut(Bytes.toBytes(100), Bytes.toBytes(1000L), 0L)));
  }
}
