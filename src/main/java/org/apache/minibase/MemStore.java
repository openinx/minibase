package org.apache.minibase;

import org.apache.log4j.Logger;
import org.apache.minibase.DiskStore.MultiIter;
import org.apache.minibase.MiniBase.Iter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemStore implements Closeable {

  private static final Logger LOG = Logger.getLogger(MemStore.class);
  public static final long MAX_FLUSH_SIZE = 16 * 1024 * 1024;

  private final LongAdder dataSize;

  private volatile ConcurrentSkipListMap<KeyValue, KeyValue> kvMap;
  private volatile ConcurrentSkipListMap<KeyValue, KeyValue> snapshot;

  private final ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock();
  private final AtomicBoolean isSnapshotFlushing = new AtomicBoolean(false);
  private ExecutorService pool = Executors.newFixedThreadPool(5);

  public MemStore() {
    dataSize = new LongAdder();
    this.kvMap = new ConcurrentSkipListMap<>();
    this.snapshot = null;
  }

  public void add(KeyValue kv) throws IOException {
    flushIfNeeded(true);
    updateLock.readLock().lock();
    try {
      KeyValue prevKeyValue;
      if ((prevKeyValue = kvMap.put(kv, kv)) == null) {
        dataSize.add(kv.getSerializeSize());
      } else {
        dataSize.add(kv.getSerializeSize() - prevKeyValue.getSerializeSize());
      }
    } finally {
      updateLock.readLock().unlock();
    }
    flushIfNeeded(false);
  }
  
  private void flushIfNeeded(boolean shouldBlocking) throws IOException {
    if (getDataSize() > MAX_FLUSH_SIZE) {
      if (isSnapshotFlushing.get()) {
        if (shouldBlocking) {
          throw new IOException("Memstore is full(" + dataSize.sum()
              + "B), please wait until the flushing is finished.");
        }
      } else if (isSnapshotFlushing.compareAndSet(false, true)) {
        pool.submit(new Flusher());
      }
    }
  }

  public long getDataSize() {
    return dataSize.sum();
  }

  @Override
  public void close() throws IOException {
  }


  private class Flusher implements Runnable {
    public Flusher() {

    }

    @Override
    public void run() {
      // Step.1 memstore snpashot
      updateLock.writeLock().lock();
      try {
        snapshot = kvMap;
        kvMap = new ConcurrentSkipListMap<>();
        dataSize.reset();
      } finally {
        updateLock.writeLock().unlock();
      }

      // Step.2 Flush the memstore to disk file.
    }
  }

  public Iter<KeyValue> iterator() throws IOException {
    return new MemStoreIter(kvMap, snapshot);
  }

  public static class IteratorWrapper implements Iter<KeyValue> {

    private Iterator<KeyValue> it;

    public IteratorWrapper(Iterator<KeyValue> it) {
      this.it = it;
    }

    @Override
    public boolean hasNext() throws IOException {
      return it != null && it.hasNext();
    }

    @Override
    public KeyValue next() throws IOException {
      return it.next();
    }
  }

  private class MemStoreIter implements Iter<KeyValue> {

    private MultiIter it;

    public MemStoreIter(NavigableMap<KeyValue, KeyValue> kvSet,
        NavigableMap<KeyValue, KeyValue> snapshot) throws IOException {
      List<IteratorWrapper> inputs = new ArrayList<>();
      if (kvSet != null && kvSet.size() > 0) {
        inputs.add(new IteratorWrapper(kvSet.values().iterator()));
      }
      if (snapshot != null && snapshot.size() > 0) {
        inputs.add(new IteratorWrapper(snapshot.values().iterator()));
      }
      it = new MultiIter(inputs.toArray(new IteratorWrapper[0]));
    }

    @Override
    public boolean hasNext() throws IOException {
      return it.hasNext();
    }

    @Override
    public KeyValue next() throws IOException {
      return it.next();
    }
  }
}
