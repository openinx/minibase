package org.apache.minibase;

import java.io.IOException;
import java.util.Iterator;

public interface MiniBase {

  public void put(byte[] key, byte[] value) throws IOException;

  public byte[] get(byte[] key) throws IOException;

  public void delete(byte[] key) throws IOException;

  public Iterator<KeyValue> scan(byte[] start, byte[] stop) throws IOException;

  public Iterator<KeyValue> scan() throws IOException;
}
