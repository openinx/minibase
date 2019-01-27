package org.apache.minibase;

import org.apache.minibase.DiskStore.DefaultCompactor;
import org.apache.minibase.DiskStore.DefaultFlusher;
import org.apache.minibase.DiskStore.MultiIter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class MStore implements MiniBase {

  private ExecutorService pool;
  private MemStore memStore;
  private DiskStore diskStore;
  private Compactor compactor;
  private AtomicLong sequenceId;

  private Config conf;

  public MiniBase open() throws IOException {
    assert conf != null;

    // initialize the thread pool;
    this.pool = Executors.newFixedThreadPool(conf.getMaxThreadPoolSize());

    // initialize the disk store.
    this.diskStore = new DiskStore(conf.getDataDir(), conf.getMaxDiskFiles());
    this.diskStore.open();
    // TODO initialize the max sequence id here.
    this.sequenceId = new AtomicLong(0);

    // initialize the memstore.
    this.memStore = new MemStore(conf, new DefaultFlusher(diskStore), pool);

    this.compactor = new DefaultCompactor(diskStore);
    this.compactor.start();
    return this;
  }

  private MStore(Config conf) {
    this.conf = conf;
  }

  public static MStore create(Config conf) {
    return new MStore(conf);
  }

  public static MStore create() {
    return create(Config.getDefault());
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    this.memStore.add(KeyValue.createPut(key, value, sequenceId.incrementAndGet()));
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    // TODO
    return new byte[0];
  }

  @Override
  public void delete(byte[] key) throws IOException {
    this.memStore.add(KeyValue.createDelete(key, sequenceId.incrementAndGet()));
  }

  @Override
  public Iter<KeyValue> scan(byte[] start, byte[] stop) throws IOException {
    List<Iter<KeyValue>> iterList = new ArrayList<>();
    iterList.add(memStore.createIterator());
    iterList.add(diskStore.createIterator());
    return new MultiIter(iterList);
  }

  @Override
  public void close() throws IOException {
    memStore.close();
    diskStore.close();
    compactor.interrupt();
  }
}
