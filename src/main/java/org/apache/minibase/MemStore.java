package org.apache.minibase;

import org.apache.log4j.Logger;
import org.apache.minibase.DiskStore.MultiIter;
import org.apache.minibase.MStore.SeekIter;
import org.apache.minibase.MiniBase.Flusher;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemStore implements Closeable {

  private static final Logger LOG = Logger.getLogger(MemStore.class);

  private final AtomicLong dataSize = new AtomicLong();

  private volatile ConcurrentSkipListMap<KeyValue, KeyValue> kvMap;
  private volatile ConcurrentSkipListMap<KeyValue, KeyValue> snapshot;

  private final ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock();
  private final AtomicBoolean isSnapshotFlushing = new AtomicBoolean(false);
  private ExecutorService pool;

  private Config conf;
  private Flusher flusher;

  public MemStore(Config conf, Flusher flusher, ExecutorService pool) {
    this.conf = conf;
    this.flusher = flusher;
    this.pool = pool;

    dataSize.set(0);
    this.kvMap = new ConcurrentSkipListMap<>();
    this.snapshot = null;
  }

  public void add(KeyValue kv) throws IOException {
    flushIfNeeded(true);
    updateLock.readLock().lock();
    try {
      KeyValue prevKeyValue;
      if ((prevKeyValue = kvMap.put(kv, kv)) == null) {
        dataSize.addAndGet(kv.getSerializeSize());
      } else {
        dataSize.addAndGet(kv.getSerializeSize() - prevKeyValue.getSerializeSize());
      }
    } finally {
      updateLock.readLock().unlock();
    }
    flushIfNeeded(false);
  }

  private void flushIfNeeded(boolean shouldBlocking) throws IOException {
    if (getDataSize() > conf.getMaxMemstoreSize()) {
      if (isSnapshotFlushing.get() && shouldBlocking) {
        throw new IOException(
                "Memstore is full, currentDataSize=" + dataSize.get() + "B, maxMemstoreSize="
                + conf.getMaxMemstoreSize() + "B, please wait until the flushing is finished.");
      } else if (isSnapshotFlushing.compareAndSet(false, true)) {
        pool.submit(new FlusherTask());
      }
    }
  }

  public long getDataSize() {
    return dataSize.get();
  }

  public boolean isFlushing() {
    return this.isSnapshotFlushing.get();
  }

  @Override
  public void close() throws IOException {
  }

  private class FlusherTask implements Runnable {
    @Override
    public void run() {
      // Step.1 memstore snpashot
      updateLock.writeLock().lock();
      try {
        snapshot = kvMap;
        // TODO MemStoreIter may find the kvMap changed ? should synchronize ?
        kvMap = new ConcurrentSkipListMap<>();
        dataSize.set(0);
      } finally {
        updateLock.writeLock().unlock();
      }

      // Step.2 Flush the memstore to disk file.
      boolean success = false;
      for (int i = 0; i < conf.getFlushMaxRetries(); i++) {
        try {
          flusher.flush(new IteratorWrapper(snapshot));
          success = true;
        } catch (IOException e) {
          LOG.error("Failed to flush memstore, retries=" + i + ", maxFlushRetries="
                    + conf.getFlushMaxRetries(),
                  e);
          if (i >= conf.getFlushMaxRetries()) {
            break;
          }
        }
      }

      // Step.3 clear the snapshot.
      if (success) {
        // TODO MemStoreIter may get a NPE because we set null here ? should synchronize ?
        snapshot = null;
        isSnapshotFlushing.compareAndSet(true, false);
      }
    }
  }

  public SeekIter<KeyValue> createIterator() throws IOException {
    return new MemStoreIter(kvMap, snapshot);
  }

  public static class IteratorWrapper implements SeekIter<KeyValue> {

    private SortedMap<KeyValue, KeyValue> sortedMap;
    private Iterator<KeyValue> it;

    public IteratorWrapper(SortedMap<KeyValue, KeyValue> sortedMap) {
      this.sortedMap = sortedMap;
      this.it = sortedMap.values().iterator();
    }

    @Override
    public boolean hasNext() throws IOException {
      return it != null && it.hasNext();
    }

    @Override
    public KeyValue next() throws IOException {
      return it.next();
    }

    @Override
    public void seekTo(KeyValue kv) throws IOException {
      it = sortedMap.tailMap(kv).values().iterator();
    }
  }

  private class MemStoreIter implements SeekIter<KeyValue> {

    private MultiIter it;

    public MemStoreIter(NavigableMap<KeyValue, KeyValue> kvSet,
                        NavigableMap<KeyValue, KeyValue> snapshot) throws IOException {
      List<IteratorWrapper> inputs = new ArrayList<>();
      if (kvSet != null && kvSet.size() > 0) {
        inputs.add(new IteratorWrapper(kvMap));
      }
      if (snapshot != null && snapshot.size() > 0) {
        inputs.add(new IteratorWrapper(snapshot));
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

    @Override
    public void seekTo(KeyValue kv) throws IOException {
      it.seekTo(kv);
    }
  }
}
