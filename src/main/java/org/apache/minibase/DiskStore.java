package org.apache.minibase;

import org.apache.minibase.DiskFile.DiskFileWriter;
import org.apache.minibase.MiniBase.Flusher;
import org.apache.minibase.MiniBase.Iter;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DiskStore implements Closeable {

  private static final Pattern DATA_FILE_RE = Pattern.compile("data\\.([0-9]+)"); // data.1
  private String dataDir;
  private List<DiskFile> diskFiles;

  public DiskStore(String dataDir) {
    this.dataDir = dataDir;
    this.diskFiles = new ArrayList<>();
  }

  private File[] listDiskFiles() {
    File f = new File(this.dataDir);
    return f.listFiles(fname -> DATA_FILE_RE.matcher(fname.getName()).matches());
  }

  public synchronized long nextDiskFileId() {
    File[] files = listDiskFiles();
    long maxFileId = 0;
    for (File f : files) {
      Matcher matcher = DATA_FILE_RE.matcher(f.getName());
      if (matcher.matches()) {
        maxFileId = Math.max(Long.parseLong(matcher.group(1)), maxFileId);
      }
    }
    return maxFileId + 1;
  }

  public synchronized void addDiskFile(DiskFile df) {
    diskFiles.add(df);
  }

  public synchronized void addDiskFile(String filename) throws IOException {
    DiskFile df = new DiskFile();
    df.open(filename);
    addDiskFile(df);
  }

  public synchronized String getNextDiskFileName() {
    return new File(this.dataDir, String.format("data.%020d", nextDiskFileId())).toString();
  }

  public void open() throws IOException {
    File[] files = listDiskFiles();
    for (File f : files) {
      DiskFile df = new DiskFile();
      df.open(f.getAbsolutePath());
      diskFiles.add(df);
    }
  }

  @Override
  public void close() throws IOException {
    IOException closedException = null;
    for (DiskFile df : diskFiles) {
      try {
        df.close();
      } catch (IOException e) {
        closedException = e;
      }
    }
    if (closedException != null) {
      throw closedException;
    }
  }

  public Iter<KeyValue> iterator() throws IOException {
    List<Iter<KeyValue>> iters = new ArrayList<>();
    diskFiles.stream().forEach(df -> iters.add(df.iterator()));
    return new MultiIter(iters);
  }

  public static class DefaultFlusher implements Flusher {
    private DiskStore diskStore;

    public DefaultFlusher(DiskStore diskStore) {
      this.diskStore = diskStore;
    }

    @Override
    public void flush(Set<KeyValue> kvSet) throws IOException {
      String fileName = diskStore.getNextDiskFileName();
      String fileTempName = diskStore.getNextDiskFileName() + ".tmp";
      try {
        try (DiskFileWriter writer = new DiskFileWriter(fileTempName)) {
          for (Iterator<KeyValue> it = kvSet.iterator(); it.hasNext();) {
            writer.append(it.next());
          }
          writer.appendIndex();
          writer.appendTrailer();
        }
        File f = new File(fileTempName);
        if (!f.renameTo(new File(fileName))) {
          throw new IOException("Rename " + fileTempName + " to " + fileName + " failed");
        }
        // TODO any concurrent issue ?
        diskStore.addDiskFile(fileName);
      } finally {
        File f = new File(fileTempName);
        if (f.exists()) {
          f.delete();
        }
      }
    }
  }

  public static class MultiIter implements Iter<KeyValue> {

    private class IterNode {
      KeyValue kv;
      Iter<KeyValue> iter;

      public IterNode(KeyValue kv, Iter<KeyValue> it) {
        this.kv = kv;
        this.iter = it;
      }
    }

    private PriorityQueue<IterNode> queue;

    public MultiIter(Iter<KeyValue> iters[]) throws IOException {
      assert iters != null;
      queue = new PriorityQueue<>(((o1, o2) -> o1.kv.compareTo(o2.kv)));
      for (int i = 0; i < iters.length; i++) {
        if (iters[i] != null && iters[i].hasNext()) {
          queue.add(new IterNode(iters[i].next(), iters[i]));
        }
      }
    }

    public MultiIter(List<Iter<KeyValue>> iters) throws IOException {
      assert iters != null;
      queue = new PriorityQueue<>(((o1, o2) -> o1.kv.compareTo(o2.kv)));
      for (Iter<KeyValue> iter : iters) {
        if (iter != null && iter.hasNext()) {
          queue.add(new IterNode(iter.next(), iter));
        }
      }
    }

    @Override
    public boolean hasNext() throws IOException {
      return queue.size() > 0;
    }

    @Override
    public KeyValue next() throws IOException {
      while (!queue.isEmpty()) {
        IterNode first = queue.poll();
        if (first.kv != null && first.iter != null) {
          if (first.iter.hasNext()) {
            queue.add(new IterNode(first.iter.next(), first.iter));
          }
          return first.kv;
        }
      }
      return null;
    }
  }

}
