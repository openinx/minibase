package org.apache.minibase;

import java.io.IOException;
import java.util.Comparator;

public class KeyValue implements Comparable<KeyValue> {

  public static final int KEY_LEN = 4;
  public static final int VAL_LEN = 4;
  private byte[] key;
  private byte[] value;

  public static KeyValue create(byte[] key, byte[] value) {
    return new KeyValue(key, value);
  }

  public KeyValue(byte[] key, byte[] value) {
    assert key != null;
    assert value != null;
    this.key = key;
    this.value = value;
  }

  public KeyValue setKey(byte[] key) {
    assert key != null;
    this.key = key;
    return this;
  }

  public KeyValue setValue(byte[] value) {
    assert value != null;
    this.value = value;
    return this;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  public byte[] toBytes() throws IOException {
    byte[] bytes = new byte[KEY_LEN + VAL_LEN + key.length + value.length];

    byte[] keyLen = Bytes.toBytes(key.length);
    System.arraycopy(keyLen, 0, bytes, 0, KEY_LEN);

    byte[] valLen = Bytes.toBytes(value.length);
    System.arraycopy(valLen, 0, bytes, KEY_LEN, VAL_LEN);

    System.arraycopy(key, 0, bytes, KEY_LEN + VAL_LEN, key.length);
    System.arraycopy(value, 0, bytes, KEY_LEN + VAL_LEN + key.length, value.length);
    return bytes;
  }

  public int compareTo(KeyValue kv) {
    if (kv == null) return 1;
    return Bytes.compare(this.getKey(), kv.getKey());
  }

  public boolean equals(Object kv) {
    if (kv == null) return false;
    if (!(kv instanceof KeyValue)) return false;
    KeyValue that = (KeyValue) kv;
    return this.compareTo(that) == 0;
  }

  public int getSerializeSize() {
    return KEY_LEN + VAL_LEN + key.length + value.length;
  }

  public static KeyValue parseFrom(byte[] bytes, int offset) throws IOException {
    if (bytes == null) {
      throw new IOException("buffer is null");
    }
    if (offset + KEY_LEN + VAL_LEN >= bytes.length) {
      throw new IOException("Invalid offset or len. offset: " + offset + ", len: " + bytes.length);
    }
    int keyLen = Bytes.toInt(Bytes.slice(bytes, offset, KEY_LEN));
    int valLen = Bytes.toInt(Bytes.slice(bytes, offset + KEY_LEN, VAL_LEN));
    byte[] key = Bytes.slice(bytes, offset + KEY_LEN + VAL_LEN, keyLen);
    byte[] val = Bytes.slice(bytes, offset + KEY_LEN + VAL_LEN + keyLen, valLen);
    return new KeyValue(key, val);
  }

  public static KeyValue parseFrom(byte[] bytes) throws IOException {
    if (bytes == null) {
      throw new IOException("buffer is null");
    }
    return parseFrom(bytes, 0);
  }

  public static class KeyValueComparator implements Comparator<KeyValue> {

    @Override
    public int compare(KeyValue a, KeyValue b) {
      if (a == b) return 0;
      if (a == null) return -1;
      if (b == null) return 1;
      return a.compareTo(b);
    }
  }
}
