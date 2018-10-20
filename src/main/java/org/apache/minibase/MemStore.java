package org.apache.minibase;

import org.apache.log4j.Logger;
import org.apache.minibase.DiskStore.MultiIter;
import org.apache.minibase.MiniBase.Flusher;
import org.apache.minibase.MiniBase.Iter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MemStore extends Thread implements Closeable {

  private static final Logger LOG = Logger.getLogger(MemStore.class);

  public static final long MAX_MEMSTORE_SIZE = 256 * 1024 * 1024L;

  private final AtomicLong memsize;
  private final AtomicBoolean snapshotExists;

  private volatile boolean running = true;
  private long flushSizeLimit = MAX_MEMSTORE_SIZE;
  private volatile ConcurrentSkipListSet<KeyValue> kvSet;
  private volatile ConcurrentSkipListSet<KeyValue> snapshot;
  private Flusher flusher;

  public MemStore(long flushSizeLimit, Flusher flusher) {
    memsize = new AtomicLong(0);
    snapshotExists = new AtomicBoolean(false);

    this.flushSizeLimit = flushSizeLimit;
    this.kvSet = new ConcurrentSkipListSet<>();
    this.snapshot = null;
    this.flusher = flusher;

    this.setDaemon(true);
  }

  public MemStore(Flusher flusher) {
    this(MAX_MEMSTORE_SIZE, flusher);
  }

  public void add(KeyValue kv) throws IOException {
    flush(flushSizeLimit);
    kvSet.add(kv);
    memsize.addAndGet(kv.size());
  }

  private void flush(long limit) throws IOException {
    while (memsize.get() > limit) {
      if (snapshotExists.compareAndSet(false, true)) {
        synchronized (snapshotExists) {
          snapshot = kvSet;
          kvSet = new ConcurrentSkipListSet<>();
          memsize.set(0);
          snapshotExists.notify();
          return;
        }
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  public void flush() throws IOException {
    flush(0);
  }

  @Override
  public void run() {
    while (running) {
      if (snapshotExists.get()) {
        try {
          if (flusher != null && snapshot != null && snapshot.size() > 0) {
            flusher.flush(snapshot);
          }
        } catch (IOException e) {
          LOG.error("MemStore flush failed: ", e);
        } finally {
          synchronized (snapshotExists) {
            if (snapshotExists.compareAndSet(true, false)) {
              snapshot = null;
            }
          }
        }
      }
      synchronized (snapshotExists) {
        try {
          snapshotExists.wait();
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    running = false;
    synchronized (snapshotExists) {
      snapshotExists.notify();
    }
  }

  public Iter<KeyValue> iterator() throws IOException {
    return new MemStoreIter(kvSet, snapshot);
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

    public MemStoreIter(Set<KeyValue> kvSet, Set<KeyValue> snapshot) throws IOException {
      List<IteratorWrapper> inputs = new ArrayList<>();
      if (kvSet != null && kvSet.size() > 0) {
        inputs.add(new IteratorWrapper(kvSet.iterator()));
      }
      if (snapshot != null && snapshot.size() > 0) {
        inputs.add(new IteratorWrapper(snapshot.iterator()));
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
