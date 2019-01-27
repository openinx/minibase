package org.apache.minibase;

import java.io.Closeable;
import java.io.IOException;

public interface MiniBase extends Closeable {

  void put(byte[] key, byte[] value) throws IOException;

  KeyValue get(byte[] key) throws IOException;

  void delete(byte[] key) throws IOException;

  /**
   * Fetch all the key values whose key located in the range [startKey, stopKey)
   *
   * @param startKey start key to scan (inclusive), if start is byte[0], it means negative
   *                 infinity.
   * @param stopKey  to stop the scan. (exclusive), if stopKey is byte[0], it means positive
   *                 infinity.
   * @return Iterator for fetching the key value one by one.
   */
  Iter<KeyValue> scan(byte[] startKey, byte[] stopKey) throws IOException;

  /**
   * Full scan the Key Value store.
   *
   * @return Iterator to fetch the key value one by one.
   * @throws IOException
   */
  default Iter<KeyValue> scan() throws IOException {
    return scan(Bytes.EMPTY_BYTES, Bytes.EMPTY_BYTES);
  }

  interface Iter<KeyValue> {
    boolean hasNext() throws IOException;

    KeyValue next() throws IOException;
  }

  interface Flusher {
    void flush(Iter<KeyValue> it) throws IOException;
  }

  abstract class Compactor extends Thread {
    public abstract void compact() throws IOException;
  }
}
