package org.apache.minibase;

import java.io.IOException;
import java.util.Iterator;

public class MiniBaseImpl implements MiniBase {
  public void put(byte[] key, byte[] value) throws IOException {

  }

  public byte[] get(byte[] key) throws IOException {
    return new byte[0];
  }

  public void delete(byte[] key) throws IOException {

  }

  public Iterator<KeyValue> scan(byte[] start, byte[] stop) throws IOException {
    return null;
  }

  public Iterator<KeyValue> scan() throws IOException {
    return null;
  }
}
