package org.apache.minibase;

import org.apache.minibase.DiskFile.DiskFileWriter;
import org.apache.minibase.DiskStore.MultiIter;
import org.apache.minibase.MStore.SeekIter;
import org.apache.minibase.MiniBase.Iter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestMultiIter {

  public static class MockIter implements SeekIter<KeyValue> {

    private int cur;
    private KeyValue[] kvs;

    public MockIter(int[] array) throws IOException {
      assert array != null;
      kvs = new KeyValue[array.length];
      for (int i = 0; i < array.length; i++) {
        String s = String.format("%05d", array[i]);
        kvs[i] = KeyValue.createPut(Bytes.toBytes(s), Bytes.toBytes(s), 1L);
      }
      cur = 0;
    }

    @Override
    public boolean hasNext() throws IOException {
      return cur < kvs.length;
    }

    @Override
    public KeyValue next() throws IOException {
      return kvs[cur++];
    }

    @Override
    public void seekTo(KeyValue kv) throws IOException {
      for (cur = 0; cur < kvs.length; cur++) {
        if (kvs[cur].compareTo(kv) >= 0) {
          break;
        }
      }
    }
  }

  @Test
  public void testMergeSort() throws IOException {
    int[] a = new int[] { 2, 5, 8, 10, 20 };
    int[] b = new int[] { 11, 12, 12 };
    MockIter iter1 = new MockIter(a);
    MockIter iter2 = new MockIter(b);
    SeekIter<KeyValue>[] iters = new SeekIter[] { iter1, iter2 };
    MultiIter multiIter = new MultiIter(iters);

    String[] results =
        new String[] { "00002", "00005", "00008", "00010", "00011", "00012", "00012", "00020" };
    int index = 0;

    while (multiIter.hasNext()) {
      KeyValue kv = multiIter.next();
      Assert.assertTrue(index < results.length);
      Assert.assertArrayEquals(kv.getKey(), Bytes.toBytes(results[index]));
      Assert.assertArrayEquals(kv.getValue(), Bytes.toBytes(results[index]));
      index++;
    }

    Assert.assertEquals(index, results.length);
  }

  @Test
  public void testMergeSort2() throws IOException {
    int[] a = new int[] {};
    int[] b = new int[] {};
    MockIter iter1 = new MockIter(a);
    MockIter iter2 = new MockIter(b);
    SeekIter<KeyValue>[] iters = new SeekIter[] { iter1, iter2 };
    MultiIter multiIter = new MultiIter(iters);

    Assert.assertFalse(multiIter.hasNext());
  }

  @Test
  public void testMergeSort3() throws IOException {
    int[] a = new int[]{};
    int[] b = new int[]{1};
    MockIter iter1 = new MockIter(a);
    MockIter iter2 = new MockIter(b);
    SeekIter<KeyValue>[] iters = new SeekIter[]{iter1, iter2};
    MultiIter multiIter = new MultiIter(iters);

    Assert.assertTrue(multiIter.hasNext());
    Assert.assertEquals(multiIter.next(),
            KeyValue.createPut(Bytes.toBytes("00001"), Bytes.toBytes("00001"), 1L));
    Assert.assertFalse(multiIter.hasNext());
  }

  @Test
  public void testMergeSort4() throws IOException {
    int[] a = new int[] {};
    int[] b = new int[] { 1, 1 };
    int[] c = new int[] { 1, 1 };
    MockIter iter1 = new MockIter(a);
    MockIter iter2 = new MockIter(b);
    MockIter iter3 = new MockIter(c);
    SeekIter<KeyValue>[] iters = new SeekIter[] { iter1, iter2, iter3 };
    MultiIter multiIter = new MultiIter(iters);

    int count = 0;
    while (multiIter.hasNext()) {
      Assert.assertEquals(multiIter.next(),
              KeyValue.createPut(Bytes.toBytes("00001"), Bytes.toBytes("00001"), 1L));
      count++;
    }
    Assert.assertEquals(count, 4);
  }

  private void testDiskFileMergeSort(String[] inputs, String output, int rowCount)
      throws IOException {
    try {
      DiskFileWriter[] writers = new DiskFileWriter[inputs.length];
      DiskFile[] readers = new DiskFile[inputs.length];
      SeekIter<KeyValue> iterArray[] = new SeekIter[inputs.length];

      for (int i = 0; i < inputs.length; i++) {
        writers[i] = new DiskFileWriter(inputs[i]);
      }
      for (int i = 0; i < rowCount; i++) {
        int k = i % inputs.length;
        writers[k].append(KeyValue.createPut(Bytes.toBytes(i), Bytes.toBytes(i), 1L));
      }
      for (int i = 0; i < inputs.length; i++) {
        writers[i].appendIndex();
        writers[i].appendTrailer();
        writers[i].close();

        // open the file
        readers[i] = new DiskFile();
        readers[i].open(inputs[i]);
        iterArray[i] = readers[i].iterator();
      }

      DiskFileWriter writer = new DiskFileWriter(output);
      MultiIter iter = new MultiIter(iterArray);
      while (iter.hasNext()) {
        writer.append(iter.next());
      }

      writer.appendIndex();
      writer.appendTrailer();
      writer.close();

      // close the readers
      for (int i = 0; i < readers.length; i++) {
        readers[i].close();
      }

      DiskFile reader = new DiskFile();
      reader.open(output);
      Iter<KeyValue> resultIter = reader.iterator();
      int count = 0;
      while (resultIter.hasNext()) {
        Assert.assertEquals(resultIter.next(),
                KeyValue.createPut(Bytes.toBytes(count), Bytes.toBytes(count), 1L));
        count++;
      }
      Assert.assertEquals(count, rowCount);
      reader.close();
    } finally {
      // Remove the dbFile.
      List<String> deleteFiles = new ArrayList<>(Arrays.asList(inputs));
      deleteFiles.add(output);
      for (String fileName : deleteFiles) {
        File f = new File(fileName);
        if (f.exists()) {
          f.delete();
        }
      }
    }
  }

  @Test
  public void testDiskFileMergeSort() throws IOException {
    testDiskFileMergeSort(new String[] { "a.db", "b.db" }, "c.db", 10);
    testDiskFileMergeSort(new String[] { "a.db" }, "b.db", 1);
    testDiskFileMergeSort(new String[] { "a.db", "b.db", "c.db" }, "d.db", 1000);
    testDiskFileMergeSort(new String[] { "a.db", "b.db", "c.db" }, "d.db", 100);
  }
}
