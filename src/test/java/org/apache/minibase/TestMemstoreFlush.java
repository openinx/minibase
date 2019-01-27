package org.apache.minibase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.minibase.MiniBase.Flusher;
import org.apache.minibase.MiniBase.Iter;
import org.junit.Assert;
import org.junit.Test;

public class TestMemstoreFlush {

  private static class SleepAndFlusher implements Flusher {

    private volatile boolean sleepNow = true;

    @Override
    public void flush(Iter<KeyValue> it) throws IOException {
      while (sleepNow) {
        try {
          Thread.sleep(100L);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    public void stopSleepNow() {
      sleepNow = false;
    }
  }

  @Test
  public void testBlockingPut() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(1);
    try {
      Config conf = new Config().setMaxMemstoreSize(1);

      SleepAndFlusher flusher = new SleepAndFlusher();
      MemStore memstore = new MemStore(conf, flusher, pool);
      memstore.add(KeyValue.createPut(Bytes.toBytes(1), Bytes.toBytes(1), 1L));
      assertEquals(memstore.getDataSize(), 25);

      // Wait 5ms for the memstore snapshot.
      Thread.sleep(5L);
      memstore.add(KeyValue.createPut(Bytes.toBytes(2), Bytes.toBytes(2), 1L));

      // Stuck in memstore flushing, will throw blocking exception.
      // because both of the memstore and snapshot are full now.
      try {
        memstore.add(KeyValue.createPut(Bytes.toBytes(3), Bytes.toBytes(3), 1L));
        fail("Should throw IOException here, because our memstore is full now");
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("Memstore is full"));
      }
      assertEquals(memstore.isFlushing(), true);

      flusher.stopSleepNow();
      Thread.sleep(200L);
      assertEquals(memstore.isFlushing(), false);
      assertEquals(memstore.getDataSize(), 25);

      memstore.add(KeyValue.createPut(Bytes.toBytes(4), Bytes.toBytes(4), 1L));
      Thread.sleep(5L);
      assertEquals(memstore.getDataSize(), 0);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testAddPutAndDelete() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(1);
    try {
      Config conf = new Config().setMaxMemstoreSize(2 * 1024 * 1024);
      MemStore store = new MemStore(conf, new SleepAndFlusher(), pool);
      for (int i = 99; i >= 0; i--) {
        KeyValue kv;
        byte[] bytes = Bytes.toBytes(i);
        if ((i & 1) != 0) {
          kv = KeyValue.createPut(bytes, bytes, i);
        } else {
          kv = KeyValue.createDelete(bytes, i);
        }
        store.add(kv);
      }
      Iter<KeyValue> it = store.createIterator();
      int index = 0;
      while (it.hasNext()) {
        KeyValue kv = it.next();
        byte[] bs = Bytes.toBytes(index);
        if ((index & 1) != 0) {
          Assert.assertEquals(kv, KeyValue.createPut(bs, bs, index));
        } else {
          Assert.assertEquals(kv, KeyValue.createDelete(bs, index));
        }
        index += 1;
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testSeqIdAndOpOrder() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(1);
    try {
      Config conf = new Config().setMaxMemstoreSize(2 * 1024 * 1024);
      MemStore store = new MemStore(conf, new SleepAndFlusher(), pool);
      byte[] bs = Bytes.toBytes(1);
      KeyValue kv1 = KeyValue.createPut(bs, bs, 1);
      KeyValue kv2 = KeyValue.createPut(bs, bs, 2);
      KeyValue kv3 = KeyValue.createDelete(bs, 2);

      store.add(kv1);
      store.add(kv2);
      store.add(kv3);

      Iter<KeyValue> it = store.createIterator();
      Assert.assertTrue(it.hasNext());
      KeyValue kv = it.next();
      Assert.assertEquals(kv, kv3);

      Assert.assertTrue(it.hasNext());
      kv = it.next();
      Assert.assertEquals(kv, kv2);

      Assert.assertTrue(it.hasNext());
      kv = it.next();
      Assert.assertEquals(kv, kv1);

      Assert.assertFalse(it.hasNext());
    } finally {
      pool.shutdownNow();
    }
  }
}
