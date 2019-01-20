package org.apache.minibase;

import org.apache.minibase.DiskStore.DefaultCompactor;
import org.apache.minibase.DiskStore.DefaultFlusher;
import org.apache.minibase.DiskStore.MultiIter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MiniBaseImpl implements MiniBase {

  private static final String DEFAULT_DATA_DIR = "MiniBase";

  private String dataDir = DEFAULT_DATA_DIR;
  private long maxMemStoreSize = 256 * 1024 * 1024L;

  private int MAX_DISK_FILES = 10;
  private int maxDiskFiles = MAX_DISK_FILES;

  private MemStore memStore;
  private DiskStore diskStore;
  private Compactor compactor;

  public MiniBaseImpl setDataDir(String datDir) {
    this.dataDir = datDir;
    return this;
  }

  public MiniBaseImpl setMaxMemStoreSize(long maxMemStoreSize) {
    this.maxMemStoreSize = maxMemStoreSize;
    return this;
  }

  public MiniBaseImpl setMaxDiskFiles(int maxDiskFiles) {
    this.maxDiskFiles = maxDiskFiles;
    return this;
  }

  public MiniBase open() throws IOException {
    diskStore = new DiskStore(this.dataDir, maxDiskFiles);
    diskStore.open();

    memStore = new MemStore(maxMemStoreSize, new DefaultFlusher(diskStore));
    memStore.start();

    compactor = new DefaultCompactor(diskStore);
    compactor.start();
    return this;
  }

  public static MiniBaseImpl create() {
    return new MiniBaseImpl();
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    this.memStore.add(KeyValue.create(key, value));
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    // TODO
    return new byte[0];
  }

  @Override
  public void delete(byte[] key) throws IOException {
    // TODO
  }

  @Override
  public Iter<KeyValue> scan(byte[] start, byte[] stop) throws IOException {
    // TODO
    return null;
  }

  @Override
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
    compactor.interrupt();
  }
}
