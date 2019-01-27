package org.apache.minibase;

import org.apache.minibase.MStore.SeekIter;

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

  public static class BlockMeta implements Comparable<BlockMeta> {

    private static final int OFFSET_SIZE = 8;
    private static final int SIZE_SIZE = 8;
    private static final int BF_LEN_SIZE = 4;

    private KeyValue lastKV;
    private long blockOffset;
    private long blockSize;
    private byte[] bloomFilter;

    /**
     * Only used for {@link SeekIter} to seek a target block meta. we only care about the lastKV, so
     * the other fields can be anything.
     *
     * @param lastKV the last key value to construct the dummy block meta.
     * @return the dummy block meta.
     */
    private static BlockMeta createSeekDummy(KeyValue lastKV) {
      return new BlockMeta(lastKV, 0L, 0L, Bytes.EMPTY_BYTES);
    }

    public BlockMeta(KeyValue lastKV, long offset, long size, byte[] bloomFilter) {
      this.lastKV = lastKV;
      this.blockOffset = offset;
      this.blockSize = size;
      this.bloomFilter = bloomFilter;
    }

    public KeyValue getLastKV() {
      return this.lastKV;
    }

    public long getBlockOffset() {
      return this.blockOffset;
    }

    public long getBlockSize() {
      return this.blockSize;
    }

    public byte[] getBloomFilter() {
      return this.bloomFilter;
    }

    public int getSerializeSize() {
      // TODO the meta no need the value of last kv, will save much bytes.
      return lastKV.getSerializeSize() + OFFSET_SIZE + SIZE_SIZE + BF_LEN_SIZE + bloomFilter.length;
    }

    public byte[] toBytes() throws IOException {
      byte[] bytes = new byte[getSerializeSize()];
      int pos = 0;

      // Encode last kv
      byte[] kvBytes = lastKV.toBytes();
      System.arraycopy(kvBytes, 0, bytes, pos, kvBytes.length);
      pos += kvBytes.length;

      // Encode blockOffset
      byte[] offsetBytes = Bytes.toBytes(blockOffset);
      System.arraycopy(offsetBytes, 0, bytes, pos, offsetBytes.length);
      pos += offsetBytes.length;

      // Encode blockSize
      byte[] sizeBytes = Bytes.toBytes(blockSize);
      System.arraycopy(sizeBytes, 0, bytes, pos, sizeBytes.length);
      pos += sizeBytes.length;

      // Encode length of bloom filter
      byte[] bfLenBytes = Bytes.toBytes(bloomFilter.length);
      System.arraycopy(bfLenBytes, 0, bytes, pos, bfLenBytes.length);
      pos += bfLenBytes.length;

      // Encode bytes of bloom filter.
      System.arraycopy(bloomFilter, 0, bytes, pos, bloomFilter.length);
      pos += bloomFilter.length;

      if (pos != bytes.length) {
        throw new IOException(
                "pos(" + pos + ") should be equal to length of bytes (" + bytes.length + ")");
      }
      return bytes;
    }

    public static BlockMeta parseFrom(byte[] buf, int offset) throws IOException {
      int pos = offset;

      // Decode last key value.
      KeyValue lastKV = KeyValue.parseFrom(buf, offset);
      pos += lastKV.getSerializeSize();

      // Decode block blockOffset
      long blockOffset = Bytes.toLong(Bytes.slice(buf, pos, OFFSET_SIZE));
      pos += OFFSET_SIZE;

      // Decode block blockSize
      long blockSize = Bytes.toLong(Bytes.slice(buf, pos, SIZE_SIZE));
      pos += SIZE_SIZE;

      // Decode blockSize of block bloom filter
      int bloomFilterSize = Bytes.toInt(Bytes.slice(buf, pos, BF_LEN_SIZE));
      pos += BF_LEN_SIZE;

      // Decode bytes of block bloom filter
      byte[] bloomFilter = Bytes.slice(buf, pos, bloomFilterSize);
      pos += bloomFilterSize;

      assert pos <= buf.length;
      return new BlockMeta(lastKV, blockOffset, blockSize, bloomFilter);
    }

    @Override
    public int compareTo(BlockMeta o) {
      return this.lastKV.compareTo(o.lastKV);
    }
  }

  public static class BlockIndexWriter {

    private List<BlockMeta> blockMetas = new ArrayList<>();
    private int totalBytes = 0;

    public void append(KeyValue lastKV, long offset, long size, byte[] bloomFilter) {
      BlockMeta meta = new BlockMeta(lastKV, offset, size, bloomFilter);
      blockMetas.add(meta);
      totalBytes += meta.getSerializeSize();
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

      totalSize += kv.getSerializeSize();
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
      return (int) crc32.getValue();
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

      // Append kv getSerializeSize.
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

      // Parse kv getSerializeSize
      int kvSize = Bytes.toInt(Bytes.slice(buffer, offset + pos, BlockWriter.KV_SIZE_LEN));
      pos += BlockWriter.KV_SIZE_LEN;

      // Parse all key value.
      for (int i = 0; i < kvSize; i++) {
        KeyValue kv = KeyValue.parseFrom(buffer, offset + pos);
        kvBuf.add(kv);
        crc32.update(buffer, offset + pos, kv.getSerializeSize());
        pos += kv.getSerializeSize();
      }

      // Parse checksum
      int checksum = Bytes.toInt(Bytes.slice(buffer, offset + pos, BlockWriter.CHECKSUM_LEN));
      pos += BlockWriter.CHECKSUM_LEN;
      assert checksum == (int) (crc32.getValue() & 0xFFFFFFFF);

      assert pos == size : "pos: " + pos + ", getSerializeSize: " + size;

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

      assert kv.getSerializeSize() + BlockWriter.KV_SIZE_LEN + BlockWriter.CHECKSUM_LEN < BLOCK_SIZE_UP_LIMIT;

      if ((currentWriter.getKeyValueCount() > 0)
          && (kv.getSerializeSize() + currentWriter.size() >= BLOCK_SIZE_UP_LIMIT)) {
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

    // TODO blockOffset may overflow.
    int offset = 0;

    do {
      BlockMeta meta = BlockMeta.parseFrom(buffer, offset);
      offset += meta.getSerializeSize();
      blockMetaSet.add(meta);
    } while (offset < buffer.length);

    assert blockMetaSet.size() == this.blockCount : "blockMetaSet.getSerializeSize:" + blockMetaSet.size()
        + ", blockCount: " + blockCount;
  }

  public String getFileName() {
    return fname;
  }

  private BlockReader load(BlockMeta meta) throws IOException {
    in.seek(meta.getBlockOffset());

    // TODO Maybe overflow.
    byte[] buffer = new byte[(int) meta.getBlockSize()];

    assert in.read(buffer) == buffer.length;
    return BlockReader.parseFrom(buffer, 0, buffer.length);
  }

  private class InternalIterator implements SeekIter<KeyValue> {

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

    @Override
    public void seekTo(KeyValue target) throws IOException {
      // Locate the smallest block meta which has the lastKV >= target.
      blockMetaIter = blockMetaSet.tailSet(BlockMeta.createSeekDummy(target)).iterator();
      currentReader = null;
      if (blockMetaIter.hasNext()) {
        currentReader = load(blockMetaIter.next());
        currentKVIndex = 0;
        // Locate the smallest KV which is greater than or equals to the given KV. We're sure that
        // we can find the currentKVIndex, because lastKV of the block is greater than or equals
        // to the target KV.
        while (currentKVIndex < currentReader.getKeyValues().size()) {
          KeyValue curKV = currentReader.getKeyValues().get(currentKVIndex);
          if (curKV.compareTo(target) >= 0) {
            break;
          }
          currentKVIndex++;
        }
        if (currentKVIndex >= currentReader.getKeyValues().size()) {
          throw new IOException("Data block mis-encoded, lastKV of the currentReader >= kv, but " +
                                "we found all kv < target");
        }
      }
    }
  }

  public SeekIter<KeyValue> iterator() {
    return new InternalIterator();
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
