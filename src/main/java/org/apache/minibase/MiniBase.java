package org.apache.minibase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

public interface MiniBase extends Closeable {

  public void put(byte[] key, byte[] value) throws IOException;

  public byte[] get(byte[] key) throws IOException;

  public void delete(byte[] key) throws IOException;

  public Iter<KeyValue> scan(byte[] start, byte[] stop) throws IOException;

  public Iter<KeyValue> scan() throws IOException;

  public static interface Flusher {
    public void flush(Set<KeyValue> kvSet) throws IOException;
  }

  public static abstract class Compactor extends Thread {
    public abstract void compact(boolean isMajor) throws IOException;
  }

  public interface Iter<KeyValue> {
    public boolean hasNext() throws IOException;

    public KeyValue next() throws IOException;
  }
}
