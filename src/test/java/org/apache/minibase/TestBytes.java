package org.apache.minibase;

import org.junit.Assert;
import org.junit.Test;

public class TestBytes {

  @Test
  public void testByte() {
    Assert.assertArrayEquals(new byte[]{'a'}, Bytes.toBytes((byte) 'a'));
  }

  @Test
  public void testToHex() {
    byte[] bytes = Bytes.toBytes(567890);
    String s = Bytes.toHex(bytes, 0, bytes.length);
    Assert.assertEquals("\\x00\\x08\\xAAR", s);
  }

  @Test
  public void testInt() {
    Assert.assertEquals(Bytes.toInt(Bytes.toBytes(123455)), 123455);
    Assert.assertEquals(Bytes.toInt(Bytes.toBytes(-1)), -1);
    Assert.assertEquals(Bytes.toInt(Bytes.toBytes(-0)), -0);
    Assert.assertEquals(Bytes.toInt(Bytes.toBytes(Integer.MAX_VALUE)), Integer.MAX_VALUE);
    Assert.assertEquals(Bytes.toInt(Bytes.toBytes(Integer.MIN_VALUE)), Integer.MIN_VALUE);
  }

  @Test
  public void testLong() {
    Assert.assertEquals(Bytes.toLong(Bytes.toBytes(123455L)), 123455L);
    Assert.assertEquals(Bytes.toLong(Bytes.toBytes(-1L)), -1L);
    Assert.assertEquals(Bytes.toLong(Bytes.toBytes(-0L)), -0L);
    Assert.assertEquals(Bytes.toLong(Bytes.toBytes(Long.MAX_VALUE)), Long.MAX_VALUE);
    Assert.assertEquals(Bytes.toLong(Bytes.toBytes(Long.MIN_VALUE)), Long.MIN_VALUE);
  }

  @Test
  public void testCompare() {
    Assert.assertEquals(Bytes.compare(null, null), 0);
    Assert.assertEquals(Bytes.compare(new byte[]{0x00}, new byte[0]), 1);
    Assert.assertEquals(Bytes.compare(new byte[]{0x00}, new byte[]{0x00}), 0);
    Assert.assertEquals(Bytes.compare(new byte[]{0x00}, null), 1);
    Assert.assertEquals(Bytes.compare(new byte[]{0x00}, new byte[]{0x01}), -1);
  }
}
