package org.apache.minibase;

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
      try {
        for (long i = start; i < end; i++) {
          db.put(Bytes.toBytes(i), Bytes.toBytes(i));
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testPut() throws IOException, InterruptedException {
    final MiniBase db =
        MiniBaseImpl.create().setDataDir(dataDir).setMaxMemStoreSize((1 << 20) * 256L).open();

    final long totalKVSize = 100000000L; // 10^8
    final int threadSize = 100;

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
      KeyValue currentKV = KeyValue.create(Bytes.toBytes(current), Bytes.toBytes(current));
      Assert.assertEquals(expected, currentKV);
      current++;
    }
    Assert.assertEquals(current, totalKVSize);

    db.close();
  }

  @Test
  public void testMajorCompact() throws IOException, InterruptedException {
    final MiniBase db =
        MiniBaseImpl.create().setDataDir(dataDir).setMaxMemStoreSize(1 << 12).setMaxDiskFiles(2)
            .open();

    final long totalKVSize = 100000000L; // 10^8
    final int threadSize = 10;

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
      KeyValue currentKV = KeyValue.create(Bytes.toBytes(current), Bytes.toBytes(current));
      Assert.assertEquals(expected, currentKV);
      current++;
    }
    Assert.assertEquals(current, totalKVSize);

    db.close();
  }
}
