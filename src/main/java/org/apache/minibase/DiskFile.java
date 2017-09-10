package org.apache.minibase;

import org.apache.minibase.MiniBase.Iter;

import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class DiskFile implements Closeable {

  public static final int BLOCK_SIZE_UP_LIMIT = 1024 * 1024 * 2;
  public static final int BLOOM_FILTER_HASH_COUNT = 3;
  public static final int BLOOM_FILTER_BITS_PER_KEY = 10;

  // fileSize(8B)+ blockCount(4B) + blockIndexOffset(8B) + blockIndexOffset(8B) + DISK_FILE_MAGIC
  // (8B)
  public static final int TRAILER_SIZE = 8 + 4 + 8 + 8 + 8;
  public static final long DISK_FILE_MAGIC = 0xFAC881234221FFA9L;

  private String fname;
  private RandomAccessFile in;
  private SortedSet<BlockMeta> blockMetaSet = new TreeSet<>();

  private long fileSize;
  private int blockCount;
  private long blockIndexOffset;
  private long blockIndexSize;

  public static class BlockMeta extends KeyValue {

    public static byte[] encodeValue(long offset, long size, byte[] bloomFilter) {
      byte[] result = Bytes.toBytes(Bytes.toBytes(offset), Bytes.toBytes(size));
      byte[] bloomFilterWithSize = Bytes.toBytes(Bytes.toBytes(bloomFilter.length), bloomFilter);
      return Bytes.toBytes(result, bloomFilterWithSize);
    }

    public static BlockMeta parseFrom(byte[] buffer, int bufferOffset) throws IOException {
      KeyValue kv = KeyValue.parseFrom(buffer, bufferOffset);
      long offset = Bytes.toLong(Bytes.slice(kv.getValue(), 0, 8));
      long size = Bytes.toLong(Bytes.slice(kv.getValue(), 0 + 8, 8));
      int bloomFilterSize = Bytes.toInt(Bytes.slice(kv.getValue(), 0 + 8 + 8, 4));
      byte[] bloomFilter = Bytes.slice(kv.getValue(), 0 + 8 + 8 + 4, bloomFilterSize);
      return new BlockMeta(kv.getKey(), offset, size, bloomFilter);
    }

    private long offset;
    private long size;
    private byte[] bloomFilter;

    public BlockMeta(byte[] lastKey, long offset, long size, byte[] bloomFilter) {
      super(lastKey, encodeValue(offset, size, bloomFilter));
      this.offset = offset;
      this.size = size;
      this.bloomFilter = bloomFilter;
    }

    public long getOffset() {
      return this.offset;
    }

    public long getSize() {
      return this.size;
    }

    public byte[] getBloomFilter() {
      return this.bloomFilter;
    }
  }

  public static class BlockIndexWriter {

    private List<BlockMeta> blockMetas = new ArrayList<>();
    private int totalBytes = 0;

    public void append(KeyValue lastKV, long offset, long size, byte[] bloomFilter) {
      BlockMeta meta = new BlockMeta(lastKV.getKey(), offset, size, bloomFilter);
      blockMetas.add(meta);
      totalBytes += meta.size();
    }

    public byte[] serialize() throws IOException {
      byte[] buffer = new byte[totalBytes];
      int pos = 0;
      for (BlockMeta meta : blockMetas) {
        byte[] metaBytes = meta.toBytes();
        System.arraycopy(metaBytes, 0, buffer, pos, metaBytes.length);
        pos += metaBytes.length;
      }
      assert pos == totalBytes;
      return buffer;
    }
  }

  public static class BlockWriter {
    public static final int KV_SIZE_LEN = 4;
    public static final int CHECKSUM_LEN = 4;

    private int totalSize;
    private List<KeyValue> kvBuf;
    private BloomFilter bloomFilter;
    private Checksum crc32;
    private KeyValue lastKV;
    private int keyValueCount;

    public BlockWriter() {
      totalSize = 0;
      kvBuf = new ArrayList<>();
      bloomFilter = new BloomFilter(BLOOM_FILTER_HASH_COUNT, BLOOM_FILTER_BITS_PER_KEY);
      crc32 = new CRC32();
    }

    public void append(KeyValue kv) throws IOException {
      // Update key value buffer
      kvBuf.add(kv);
      lastKV = kv;

      // Update checksum
      byte[] buf = kv.toBytes();
      crc32.update(buf, 0, buf.length);

      totalSize += kv.size();
      keyValueCount += 1;
    }

    public byte[] getBloomFilter() {
      byte[][] bytes = new byte[kvBuf.size()][];
      for (int i = 0; i < kvBuf.size(); i++) {
        bytes[i] = kvBuf.get(i).getKey();
      }
      return bloomFilter.generate(bytes);
    }

    public int getChecksum() {
      return (int) (crc32.getValue() & 0xFFFFFFFF);
    }

    public KeyValue getLastKV() {
      return this.lastKV;
    }

    public int size() {
      return KV_SIZE_LEN + totalSize + CHECKSUM_LEN;
    }

    public int getKeyValueCount() {
      return keyValueCount;
    }

    public byte[] serialize() throws IOException {
      byte[] buffer = new byte[size()];
      int pos = 0;

      // Append kv size.
      byte[] kvSize = Bytes.toBytes(kvBuf.size());
      System.arraycopy(kvSize, 0, buffer, pos, kvSize.length);
      pos += kvSize.length;

      // Append all the key value
      for (int i = 0; i < kvBuf.size(); i++) {
        byte[] kv = kvBuf.get(i).toBytes();
        System.arraycopy(kv, 0, buffer, pos, kv.length);
        pos += kv.length;
      }

      // Append checksum.
      byte[] checksum = Bytes.toBytes(this.getChecksum());
      System.arraycopy(checksum, 0, buffer, pos, checksum.length);
      pos += checksum.length;

      assert pos == size();
      return buffer;
    }
  }

  public static class BlockReader {

    private List<KeyValue> kvBuf;

    public BlockReader(List<KeyValue> kvBuf) {
      this.kvBuf = kvBuf;
    }

    public static BlockReader parseFrom(byte[] buffer, int offset, int size) throws IOException {
      int pos = 0;
      List<KeyValue> kvBuf = new ArrayList<KeyValue>();
      Checksum crc32 = new CRC32();

      // Parse kv size
      int kvSize = Bytes.toInt(Bytes.slice(buffer, offset + pos, BlockWriter.KV_SIZE_LEN));
      pos += BlockWriter.KV_SIZE_LEN;

      // Parse all key value.
      for (int i = 0; i < kvSize; i++) {
        KeyValue kv = KeyValue.parseFrom(buffer, offset + pos);
        kvBuf.add(kv);
        crc32.update(buffer, offset + pos, kv.size());
        pos += kv.size();
      }

      // Parse checksum
      int checksum = Bytes.toInt(Bytes.slice(buffer, offset + pos, BlockWriter.CHECKSUM_LEN));
      pos += BlockWriter.CHECKSUM_LEN;
      assert checksum == (int) (crc32.getValue() & 0xFFFFFFFF);

      assert pos == size : "pos: " + pos + ", size: " + size;

      return new BlockReader(kvBuf);
    }

    public List<KeyValue> getKeyValues() {
      return kvBuf;
    }
  }

  public static class DiskFileWriter implements Closeable {
    private String fname;

    private long currentOffset;
    private BlockIndexWriter indexWriter;
    private BlockWriter currentWriter;
    private FileOutputStream out;

    private long fileSize = 0;
    private int blockCount = 0;
    private long blockIndexOffset = 0;
    private long blockIndexSize = 0;

    public DiskFileWriter(String fname) throws IOException {
      this.fname = fname;

      File f = new File(this.fname);
      f.createNewFile();
      out = new FileOutputStream(f, true);
      currentOffset = 0;
      indexWriter = new BlockIndexWriter();
      currentWriter = new BlockWriter();
    }

    private void switchNextBlockWriter() throws IOException {
      assert currentWriter.getLastKV() != null;

      byte[] buffer = currentWriter.serialize();
      out.write(buffer);
      indexWriter.append(currentWriter.getLastKV(), currentOffset, buffer.length,
        currentWriter.getBloomFilter());

      currentOffset += buffer.length;
      blockCount += 1;

      // switch to the next block.
      currentWriter = new BlockWriter();
    }

    public void append(KeyValue kv) throws IOException {
      if (kv == null) return;

      assert kv.size() + BlockWriter.KV_SIZE_LEN + BlockWriter.CHECKSUM_LEN < BLOCK_SIZE_UP_LIMIT;

      if ((currentWriter.getKeyValueCount() > 0)
          && (kv.size() + currentWriter.size() >= BLOCK_SIZE_UP_LIMIT)) {
        switchNextBlockWriter();
      }

      currentWriter.append(kv);
    }

    public void appendIndex() throws IOException {
      if (currentWriter.getKeyValueCount() > 0) {
        switchNextBlockWriter();
      }

      byte[] buffer = indexWriter.serialize();
      blockIndexOffset = currentOffset;
      blockIndexSize = buffer.length;

      out.write(buffer);

      currentOffset += buffer.length;
    }

    public void appendTrailer() throws IOException {
      fileSize = currentOffset + TRAILER_SIZE;

      // fileSize(8B)
      byte[] buffer = Bytes.toBytes(fileSize);
      out.write(buffer);

      // blockCount(4B)
      buffer = Bytes.toBytes(blockCount);
      out.write(buffer);

      // blockIndexOffset(8B)
      buffer = Bytes.toBytes(blockIndexOffset);
      out.write(buffer);

      // blockIndexSize(8B)
      buffer = Bytes.toBytes(blockIndexSize);
      out.write(buffer);

      // DISK_FILE_MAGIC(8B)
      buffer = Bytes.toBytes(DISK_FILE_MAGIC);
      out.write(buffer);
    }

    public void close() throws IOException {
      if (out != null) {
        try {
          out.flush();
          FileDescriptor fd = out.getFD();
          fd.sync();
        } finally {
          out.close();
        }
      }
    }
  }

  public void open(String filename) throws IOException {
    this.fname = filename;

    File f = new File(fname);
    this.in = new RandomAccessFile(f, "r");

    this.fileSize = f.length();
    assert fileSize > TRAILER_SIZE;
    in.seek(fileSize - TRAILER_SIZE);

    byte[] buffer = new byte[8];
    assert in.read(buffer) == buffer.length;
    assert this.fileSize == Bytes.toLong(buffer);

    buffer = new byte[4];
    assert in.read(buffer) == buffer.length;
    this.blockCount = Bytes.toInt(buffer);

    buffer = new byte[8];
    assert in.read(buffer) == buffer.length;
    this.blockIndexOffset = Bytes.toLong(buffer);

    buffer = new byte[8];
    assert in.read(buffer) == buffer.length;
    this.blockIndexSize = Bytes.toLong(buffer);

    buffer = new byte[8];
    assert in.read(buffer) == buffer.length;
    assert DISK_FILE_MAGIC == Bytes.toLong(buffer);

    // TODO Maybe a large memory, and overflow
    buffer = new byte[(int) blockIndexSize];
    in.seek(blockIndexOffset);
    assert in.read(buffer) == blockIndexSize;

    // TODO offset may overflow.
    int offset = 0;

    do {
      BlockMeta meta = BlockMeta.parseFrom(buffer, offset);
      offset += meta.size();
      blockMetaSet.add(meta);
    } while (offset < buffer.length);

    assert blockMetaSet.size() == this.blockCount : "blockMetaSet.size:" + blockMetaSet.size()
        + ", blockCount: " + blockCount;
  }

  private BlockReader load(BlockMeta meta) throws IOException {
    in.seek(meta.getOffset());

    // TODO Maybe overflow.
    byte[] buffer = new byte[(int) meta.getSize()];

    assert in.read(buffer) == buffer.length;
    return BlockReader.parseFrom(buffer, 0, buffer.length);
  }

  private class InternalIterator implements Iter<KeyValue> {

    private int currentKVIndex = 0;
    private BlockReader currentReader;
    private Iterator<BlockMeta> blockMetaIter;

    public InternalIterator() {
      currentReader = null;
      blockMetaIter = blockMetaSet.iterator();
    }

    private boolean nextBlockReader() throws IOException {
      if (blockMetaIter.hasNext()) {
        currentReader = load(blockMetaIter.next());
        currentKVIndex = 0;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public boolean hasNext() throws IOException {
      if (currentReader == null) {
        return nextBlockReader();
      } else {
        if (currentKVIndex < currentReader.getKeyValues().size()) {
          return true;
        } else {
          return nextBlockReader();
        }
      }
    }

    @Override
    public KeyValue next() throws IOException {
      return currentReader.getKeyValues().get(currentKVIndex++);
    }
  }

  public Iter<KeyValue> iterator() {
    return new InternalIterator();
  }

  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
