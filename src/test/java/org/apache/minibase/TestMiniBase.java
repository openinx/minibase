package org.apache.minibase;

import org.apache.minibase.KeyValue.Op;
import org.apache.minibase.MStore.ScanIter;
import org.apache.minibase.MStore.SeekIter;
import org.apache.minibase.MiniBase.Iter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  @Test
  public void testMixedOp() throws Exception {
    Config conf = new Config().setDataDir(dataDir).setMaxMemstoreSize(2 * 1024 * 1024);
    MiniBase db = MStore.create(conf).open();

    byte[] A = Bytes.toBytes("A");
    byte[] B = Bytes.toBytes("B");
    byte[] C = Bytes.toBytes("C");

    db.put(A, A);
    Assert.assertArrayEquals(db.get(A).getValue(), A);

    db.delete(A);
    Assert.assertNull(db.get(A));

    db.put(A, B);
    Assert.assertArrayEquals(db.get(A).getValue(), B);

    db.put(B, A);
    Assert.assertArrayEquals(db.get(B).getValue(), A);

    db.put(B, B);
    Assert.assertArrayEquals(db.get(B).getValue(), B);

    db.put(C, C);
    Assert.assertArrayEquals(db.get(C).getValue(), C);

    db.delete(B);
    Assert.assertNull(db.get(B));
  }

  static class MockSeekIter implements SeekIter<KeyValue> {

    private int curIdx = 0;
    private List<KeyValue> list;

    public MockSeekIter(List<KeyValue> list) {
      this.list = list;
    }

    @Override
    public void seekTo(KeyValue kv) throws IOException {
      throw new IOException("Not implemented");
    }

    @Override
    public boolean hasNext() throws IOException {
      return curIdx < list.size();
    }

    @Override
    public KeyValue next() throws IOException {
      return list.get(curIdx++);
    }
  }


  @Test
  public void testScanIter() throws Exception {
    List<KeyValue> list = new ArrayList<>();
    byte[] A = Bytes.toBytes("A");
    byte[] B = Bytes.toBytes("B");
    byte[] C = Bytes.toBytes("C");
    list.add(KeyValue.createDelete(A, 100));
    list.add(KeyValue.createDelete(A, 100));
    list.add(KeyValue.createPut(A, A, 100));
    list.add(KeyValue.createDelete(A, 99));
    list.add(KeyValue.createDelete(A, 99));
    list.add(KeyValue.createPut(A, A, 99));
    list.add(KeyValue.createPut(A, A, 99));

    list.add(KeyValue.createPut(B, B, 100));
    list.add(KeyValue.createPut(B, B, 99));
    list.add(KeyValue.createPut(B, B, 99));

    list.add(KeyValue.createPut(C, C, 80));
    list.add(KeyValue.createDelete(C, 1));

    ScanIter scan = new ScanIter(null, new MockSeekIter(list));
    Assert.assertTrue(scan.hasNext());
    Assert.assertEquals(scan.next(), KeyValue.createPut(B, B, 100));
    Assert.assertTrue(scan.hasNext());
    Assert.assertEquals(scan.next(), KeyValue.createPut(C, C, 80));
    Assert.assertFalse(scan.hasNext());

    scan = new ScanIter(KeyValue.createPut(B, B, 100), new MockSeekIter(list));
    Assert.assertFalse(scan.hasNext());

    scan = new ScanIter(KeyValue.createPut(C, C, 100), new MockSeekIter(list));
    Assert.assertTrue(scan.hasNext());
    Assert.assertEquals(scan.next(), KeyValue.createPut(B, B, 100));
    Assert.assertFalse(scan.hasNext());
  }
}
