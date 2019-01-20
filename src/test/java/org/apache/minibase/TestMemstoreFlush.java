package org.apache.minibase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.minibase.MiniBase.Flusher;
import org.apache.minibase.MiniBase.Iter;
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
      memstore.add(new KeyValue(Bytes.toBytes(1), Bytes.toBytes(1)));
      assertEquals(memstore.getDataSize(), 16);

      // Wait 5ms for the memstore snapshot.
      Thread.sleep(5L);
      memstore.add(new KeyValue(Bytes.toBytes(2), Bytes.toBytes(2)));

      // Stuck in memstore flushing, will throw blocking exception.
      // because both of the memstore and snapshot are full now.
      try {
        memstore.add(new KeyValue(Bytes.toBytes(3), Bytes.toBytes(3)));
        fail("Should throw IOException here, because our memstore is full now");
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("Memstore is full"));
      }
      assertEquals(memstore.isFlushing(), true);

      flusher.stopSleepNow();
      Thread.sleep(200L);
      assertEquals(memstore.isFlushing(), false);
      assertEquals(memstore.getDataSize(), 16);

      memstore.add(new KeyValue(Bytes.toBytes(4), Bytes.toBytes(4)));
      Thread.sleep(5L);
      assertEquals(memstore.getDataSize(), 0);
    } finally {
      pool.shutdownNow();
    }
  }
}
