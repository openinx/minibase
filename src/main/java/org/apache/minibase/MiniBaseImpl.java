package org.apache.minibase;

import org.apache.minibase.DiskStore.DefaultFlusher;
import org.apache.minibase.DiskStore.MultiIter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MiniBaseImpl implements MiniBase {

  private static final String DEFAULT_DATA_DIR = "MiniBase";
  private String dataDir = DEFAULT_DATA_DIR;
  private long maxMemStoreSize = 256 * 1024 * 1024L;

  private MemStore memStore;
  private DiskStore diskStore;

  public MiniBaseImpl setDataDir(String datDir) {
    this.dataDir = datDir;
    return this;
  }

  public MiniBaseImpl setMaxMemStoreSize(long maxMemStoreSize) {
    this.maxMemStoreSize = maxMemStoreSize;
    return this;
  }

  public MiniBase open() throws IOException {
    diskStore = new DiskStore(this.dataDir);
    diskStore.open();

    memStore = new MemStore(maxMemStoreSize, new DefaultFlusher(diskStore));
    memStore.start();
    return this;
  }

  public static MiniBaseImpl create() {
    return new MiniBaseImpl();
  }

  public void put(byte[] key, byte[] value) throws IOException {
    this.memStore.add(KeyValue.create(key, value));
  }

  public byte[] get(byte[] key) throws IOException {
    // TODO
    return new byte[0];
  }

  public void delete(byte[] key) throws IOException {
    // TODO
  }

  public Iter<KeyValue> scan(byte[] start, byte[] stop) throws IOException {
    // TODO
    return null;
  }

  public Iter<KeyValue> scan() throws IOException {
    List<Iter<KeyValue>> iterList = new ArrayList<>();
    iterList.add(memStore.iterator());
    iterList.add(diskStore.iterator());
    return new MultiIter(iterList);
  }

  @Override
  public void close() throws IOException {
    memStore.flush();
    memStore.close();
    diskStore.close();
  }
}
