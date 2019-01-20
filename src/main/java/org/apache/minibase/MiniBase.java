package org.apache.minibase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

public interface MiniBase extends Closeable {

  void put(byte[] key, byte[] value) throws IOException;

  byte[] get(byte[] key) throws IOException;

  void delete(byte[] key) throws IOException;

  Iter<KeyValue> scan(byte[] start, byte[] stop) throws IOException;

  Iter<KeyValue> scan() throws IOException;

  interface Flusher {
    void flush(Set<KeyValue> kvSet) throws IOException;
  }

  abstract class Compactor extends Thread {
    public abstract void compact(boolean isMajor) throws IOException;
  }

  interface Iter<KeyValue> {
    boolean hasNext() throws IOException;

    KeyValue next() throws IOException;

    /**
     * Seek to the largest key value which is less than or equal to the target key value.
     * @param kv target key value to seek
     * @throws IOException error to throw if fail to read file or memstore.
     */
    //void seekTo(KeyValue kv) throws IOException;
  }
}
