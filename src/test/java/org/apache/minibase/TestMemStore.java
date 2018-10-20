package org.apache.minibase;

import org.apache.minibase.MiniBase.Flusher;
import org.apache.minibase.MiniBase.Iter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMemStore {

  static class MockFlusher implements Flusher {

    private final AtomicInteger flushCounter = new AtomicInteger(0);

    @Override
    public void flush(Set<KeyValue> kvSet) throws IOException {
      flushCounter.incrementAndGet();
      while (true) {
        // block forever
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testInMemorySnapshot() throws Exception {
    long flushSizeLimit = 2 * 1024 * 1024L;
    List<KeyValue> list = new ArrayList<>();
    for (long i = 0, currentSize = 0;; i++) {
      KeyValue kv = KeyValue.create(Bytes.toBytes(i), Bytes.toBytes(i));
      list.add(kv);
      currentSize += kv.size();
      if (currentSize > flushSizeLimit + flushSizeLimit / 2) {
        break;
      }
    }

    MockFlusher flusher = new MockFlusher();
    MemStore store = new MemStore(flushSizeLimit, flusher);
    store.start();

    Assert.assertEquals(flusher.flushCounter.get(), 0);

    for (KeyValue kv : list) {
      store.add(kv);
    }

    Assert.assertEquals(flusher.flushCounter.get(), 1);

    Iter<KeyValue> iter = store.iterator();
    for (KeyValue kv : list) {
      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(iter.next(), kv);
    }
    Assert.assertFalse(iter.hasNext());
    store.close();
  }
}
