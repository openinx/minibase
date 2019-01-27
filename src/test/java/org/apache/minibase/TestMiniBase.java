package org.apache.minibase;

import org.apache.minibase.KeyValue.Op;
import org.apache.minibase.MiniBase.Iter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestMiniBase {

  private String dataDir;

  @Before
  public void setUp() {
    dataDir = "target/minihbase-" + System.currentTimeMillis();
    File f = new File(dataDir);
    Assert.assertTrue(f.mkdirs());
  }

  @After
  public void tearDown() {
  }

  private static class WriterThread extends Thread {

    private long start, end;
    private MiniBase db;

    public WriterThread(MiniBase db, long start, long end) {
      this.db = db;
      this.start = start;
      this.end = end;
    }

    public void run() {
      for (long i = start; i < end; i++) {
        int retries = 0;
        while (retries < 50) {
          try {
            db.put(Bytes.toBytes(i), Bytes.toBytes(i));
            break;
          } catch (IOException e) {
            // Memstore maybe full, so let's retry.
            retries++;
            try {
              Thread.sleep(100 * retries);
            } catch (InterruptedException e1) {
            }
          }
        }
      }
    }
  }

  @Test
  public void testPut() throws IOException, InterruptedException {
    // Set maxMemstoreSize to 64B, which make the memstore flush frequently.
    Config conf = new Config().setDataDir(dataDir).setMaxMemstoreSize(1).setFlushMaxRetries(1)
            .setMaxDiskFiles(10);
    final MiniBase db = MStore.create(conf).open();

    final long totalKVSize = 100L;
    final int threadSize = 5;

    WriterThread[] writers = new WriterThread[threadSize];
    for (int i = 0; i < threadSize; i++) {
      long kvPerThread = totalKVSize / threadSize;
      writers[i] = new WriterThread(db, i * kvPerThread, (i + 1) * kvPerThread);
      writers[i].start();
    }

    for (int i = 0; i < threadSize; i++) {
      writers[i].join();
    }

    Iter<KeyValue> kv = db.scan();
    long current = 0;
    while (kv.hasNext()) {
      KeyValue expected = kv.next();
      KeyValue currentKV = KeyValue.createPut(Bytes.toBytes(current), Bytes.toBytes(current), 0L);
      Assert.assertArrayEquals(expected.getKey(), currentKV.getKey());
      Assert.assertArrayEquals(expected.getValue(), currentKV.getValue());
      Assert.assertEquals(expected.getOp(), Op.Put);

      long sequenceId = expected.getSequenceId();
      Assert.assertTrue("SequenceId: " + sequenceId, sequenceId > 0);
      current++;
    }
    Assert.assertEquals(current, totalKVSize);
    db.close();
  }
}
