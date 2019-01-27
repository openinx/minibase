package org.apache.minibase;

import java.io.IOException;
import java.util.Comparator;

public class KeyValue implements Comparable<KeyValue> {

  public static final int RAW_KEY_LEN_SIZE = 4;
  public static final int VAL_LEN_SIZE = 4;
  public static final int OP_SIZE = 1;
  public static final int SEQ_ID_SIZE = 8;
  public static final KeyValueComparator KV_CMP = new KeyValueComparator();

  private byte[] key;
  private byte[] value;
  private Op op;
  private long sequenceId;

  public enum Op {
    Put((byte) 0),
    Delete((byte) 1);

    private byte code;

    Op(byte code) {
      this.code = code;
    }

    public static Op code2Op(byte code) {
      switch (code) {
        case 0:
          return Put;
        case 1:
          return Delete;
        default:
          throw new IllegalArgumentException("Unknown code: " + code);
      }
    }

    public byte getCode() {
      return this.code;
    }
  }

  public static KeyValue create(byte[] key, byte[] value, Op op, long sequenceId) {
    return new KeyValue(key, value, op, sequenceId);
  }

  public static KeyValue createPut(byte[] key, byte[] value, long sequenceId) {
    return KeyValue.create(key, value, Op.Put, sequenceId);
  }

  public static KeyValue createDelete(byte[] key, long sequenceId) {
    return KeyValue.create(key, Bytes.EMPTY_BYTES, Op.Delete, sequenceId);
  }

  private KeyValue(byte[] key, byte[] value, Op op, long sequenceId) {
    assert key != null;
    assert value != null;
    assert op != null;
    assert sequenceId >= 0;
    this.key = key;
    this.value = value;
    this.op = op;
    this.sequenceId = sequenceId;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  public Op getOp() {
    return this.op;
  }

  public long getSequenceId() {
    return this.sequenceId;
  }

  private int getRawKeyLen() {
    return key.length + OP_SIZE + SEQ_ID_SIZE;
  }

  public byte[] toBytes() throws IOException {
    int rawKeyLen = getRawKeyLen();
    int pos = 0;
    byte[] bytes = new byte[getSerializeSize()];

    // Encode raw key length
    byte[] rawKeyLenBytes = Bytes.toBytes(rawKeyLen);
    System.arraycopy(rawKeyLenBytes, 0, bytes, pos, RAW_KEY_LEN_SIZE);
    pos += RAW_KEY_LEN_SIZE;

    // Encode value length.
    byte[] valLen = Bytes.toBytes(value.length);
    System.arraycopy(valLen, 0, bytes, pos, VAL_LEN_SIZE);
    pos += VAL_LEN_SIZE;

    // Encode key
    System.arraycopy(key, 0, bytes, pos, key.length);
    pos += key.length;

    // Encode Op
    bytes[pos] = op.getCode();
    pos += 1;

    // Encode sequenceId
    byte[] seqIdBytes = Bytes.toBytes(sequenceId);
    System.arraycopy(seqIdBytes, 0, bytes, pos, seqIdBytes.length);
    pos += seqIdBytes.length;

    // Encode value
    System.arraycopy(value, 0, bytes, pos, value.length);
    return bytes;
  }

  @Override
  public int compareTo(KeyValue kv) {
    if (kv == null) {
      throw new IllegalArgumentException("kv to compare should be null");
    }
    int ret = Bytes.compare(this.key, kv.key);
    if (ret != 0) {
      return ret;
    }
    if (this.sequenceId != kv.sequenceId) {
      return this.sequenceId > kv.sequenceId ? -1 : 1;
    }
    if (this.op != kv.op) {
      return this.op.getCode() > kv.op.getCode() ? -1 : 1;
    }
    return 0;
  }

  @Override
  public boolean equals(Object kv) {
    if (kv == null) return false;
    if (!(kv instanceof KeyValue)) return false;
    KeyValue that = (KeyValue) kv;
    return this.compareTo(that) == 0;
  }

  public int getSerializeSize() {
    return RAW_KEY_LEN_SIZE + VAL_LEN_SIZE + getRawKeyLen() + value.length;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("key=").append(Bytes.toHex(this.key)).append("/op=").append(op).append
            ("/sequenceId=").append(this.sequenceId).append("/value=").append(Bytes.toHex(this
            .value));
    return sb.toString();
  }

  public static KeyValue parseFrom(byte[] bytes, int offset) throws IOException {
    if (bytes == null) {
      throw new IOException("buffer is null");
    }
    if (offset + RAW_KEY_LEN_SIZE + VAL_LEN_SIZE >= bytes.length) {
      throw new IOException("Invalid offset or len. offset: " + offset + ", len: " + bytes.length);
    }
    // Decode raw key length
    int pos = offset;
    int rawKeyLen = Bytes.toInt(Bytes.slice(bytes, pos, RAW_KEY_LEN_SIZE));
    pos += RAW_KEY_LEN_SIZE;

    // Decode value length
    int valLen = Bytes.toInt(Bytes.slice(bytes, pos, VAL_LEN_SIZE));
    pos += VAL_LEN_SIZE;

    // Decode key
    int keyLen = rawKeyLen - OP_SIZE - SEQ_ID_SIZE;
    byte[] key = Bytes.slice(bytes, pos, keyLen);
    pos += keyLen;

    // Decode Op
    Op op = Op.code2Op(bytes[pos]);
    pos += 1;

    // Decode sequenceId
    long sequenceId = Bytes.toLong(Bytes.slice(bytes, pos, SEQ_ID_SIZE));
    pos += SEQ_ID_SIZE;

    // Decode value.
    byte[] val = Bytes.slice(bytes, pos, valLen);
    return create(key, val, op, sequenceId);
  }

  public static KeyValue parseFrom(byte[] bytes) throws IOException {
    return parseFrom(bytes, 0);
  }

  private static class KeyValueComparator implements Comparator<KeyValue> {

    @Override
    public int compare(KeyValue a, KeyValue b) {
      if (a == b) return 0;
      if (a == null) return -1;
      if (b == null) return 1;
      return a.compareTo(b);
    }
  }
}
