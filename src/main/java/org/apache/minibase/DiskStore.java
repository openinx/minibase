package org.apache.minibase;

import org.apache.log4j.Logger;
import org.apache.minibase.DiskFile.DiskFileWriter;
import org.apache.minibase.MStore.SeekIter;
import org.apache.minibase.MiniBase.Compactor;
import org.apache.minibase.MiniBase.Flusher;
import org.apache.minibase.MiniBase.Iter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DiskStore implements Closeable {

  private static final Logger LOG = Logger.getLogger(DiskStore.class);
  private static final String FILE_NAME_TMP_SUFFIX = ".tmp";
  private static final String FILE_NAME_ARCHIVE_SUFFIX = ".archive";
  private static final Pattern DATA_FILE_RE = Pattern.compile("data\\.([0-9]+)"); // data.1

  private String dataDir;
  private List<DiskFile> diskFiles;

  private int maxDiskFiles;
  private volatile AtomicLong maxFileId;

  public DiskStore(String dataDir, int maxDiskFiles) {
    this.dataDir = dataDir;
    this.diskFiles = new ArrayList<>();
    this.maxDiskFiles = maxDiskFiles;
  }

  private File[] listDiskFiles() {
    File f = new File(this.dataDir);
    return f.listFiles(fname -> DATA_FILE_RE.matcher(fname.getName()).matches());
  }

  public synchronized long getMaxDiskId() {
    // TODO can we save the maxFileId ? and next time, need not to traverse the disk file.
    File[] files = listDiskFiles();
    long maxFileId = -1L;
    for (File f : files) {
      Matcher matcher = DATA_FILE_RE.matcher(f.getName());
      if (matcher.matches()) {
        maxFileId = Math.max(Long.parseLong(matcher.group(1)), maxFileId);
      }
    }
    return maxFileId;
  }

  public synchronized long nextDiskFileId() {
    return maxFileId.incrementAndGet();
  }

  public void addDiskFile(DiskFile df) {
    synchronized (diskFiles) {
      diskFiles.add(df);
    }
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
    maxFileId = new AtomicLong(getMaxDiskId());
  }

  public List<DiskFile> getDiskFiles() {
    synchronized (diskFiles) {
      return new ArrayList<>(diskFiles);
    }
  }

  public void removeDiskFiles(Collection<DiskFile> files) {
    synchronized (diskFiles) {
      diskFiles.removeAll(files);
    }
  }

  public long getMaxDiskFiles() {
    return this.maxDiskFiles;
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

  public SeekIter<KeyValue> createIterator(List<DiskFile> diskFiles) throws IOException {
    List<SeekIter<KeyValue>> iters = new ArrayList<>();
    diskFiles.forEach(df -> iters.add(df.iterator()));
    return new MultiIter(iters);
  }

  public SeekIter<KeyValue> createIterator() throws IOException {
    return createIterator(getDiskFiles());
  }

  public static class DefaultFlusher implements Flusher {
    private DiskStore diskStore;

    public DefaultFlusher(DiskStore diskStore) {
      this.diskStore = diskStore;
    }

    @Override
    public void flush(Iter<KeyValue> it) throws IOException {
      String fileName = diskStore.getNextDiskFileName();
      String fileTempName = fileName + FILE_NAME_TMP_SUFFIX;
      try {
        try (DiskFileWriter writer = new DiskFileWriter(fileTempName)) {
          while (it.hasNext()) {
            writer.append(it.next());
          }
          writer.appendIndex();
          writer.appendTrailer();
        }
        File f = new File(fileTempName);
        if (!f.renameTo(new File(fileName))) {
          throw new IOException(
              "Rename " + fileTempName + " to " + fileName + " failed when flushing");
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

  public static class DefaultCompactor extends Compactor {
    private DiskStore diskStore;
    private volatile boolean running = true;

    public DefaultCompactor(DiskStore diskStore) {
      this.diskStore = diskStore;
      this.setDaemon(true);
    }

    private void performCompact(List<DiskFile> filesToCompact) throws IOException {
      String fileName = diskStore.getNextDiskFileName();
      String fileTempName = fileName + FILE_NAME_TMP_SUFFIX;
      try {
        try (DiskFileWriter writer = new DiskFileWriter(fileTempName)) {
          for (Iter<KeyValue> it = diskStore.createIterator(filesToCompact); it.hasNext();) {
            writer.append(it.next());
          }
          writer.appendIndex();
          writer.appendTrailer();
        }
        File f = new File(fileTempName);
        if (!f.renameTo(new File(fileName))) {
          throw new IOException("Rename " + fileTempName + " to " + fileName + " failed");
        }

        // Rename the data files to archive files.
        // TODO when rename the files, will we effect the scan ?
        for (DiskFile df : filesToCompact) {
          df.close();
          File file = new File(df.getFileName());
          File archiveFile = new File(df.getFileName() + FILE_NAME_ARCHIVE_SUFFIX);
          if (!file.renameTo(archiveFile)) {
            LOG.error("Rename " + df.getFileName() + " to " + archiveFile.getName() + " failed.");
          }
        }
        diskStore.removeDiskFiles(filesToCompact);

        // TODO any concurrent issue ?
        diskStore.addDiskFile(fileName);
      } finally {
        File f = new File(fileTempName);
        if (f.exists()) {
          f.delete();
        }
      }
    }

    @Override
    public void compact() throws IOException {
      List<DiskFile> filesToCompact = new ArrayList<>();
      filesToCompact.addAll(diskStore.getDiskFiles());
      performCompact(filesToCompact);
    }

    public void run() {
      while (running) {
        try {
          boolean isCompacted = false;
          if (diskStore.getDiskFiles().size() > diskStore.getMaxDiskFiles()) {
            performCompact(diskStore.getDiskFiles());
            isCompacted = true;
          }
          if (!isCompacted) {
            Thread.sleep(1000);
          }
        } catch (IOException e) {
          e.printStackTrace();
          LOG.error("Major compaction failed: ", e);
        } catch (InterruptedException ie) {
          LOG.error("InterruptedException happened, stop running: ", ie);
          break;
        }
      }
    }

    public void stopRunning() {
      this.running = false;
    }
  }

  public static class MultiIter implements SeekIter<KeyValue> {

    private class IterNode {
      KeyValue kv;
      SeekIter<KeyValue> iter;

      public IterNode(KeyValue kv, SeekIter<KeyValue> it) {
        this.kv = kv;
        this.iter = it;
      }
    }

    private SeekIter<KeyValue> iters[];
    private PriorityQueue<IterNode> queue;

    public MultiIter(SeekIter<KeyValue> iters[]) throws IOException {
      assert iters != null;
      this.iters = iters; // Used for seekTo
      this.queue = new PriorityQueue<>(((o1, o2) -> o1.kv.compareTo(o2.kv)));
      for (int i = 0; i < iters.length; i++) {
        if (iters[i] != null && iters[i].hasNext()) {
          queue.add(new IterNode(iters[i].next(), iters[i]));
        }
      }
    }

    @SuppressWarnings("unchecked")
    public MultiIter(List<SeekIter<KeyValue>> iters) throws IOException {
      this(iters.toArray(new SeekIter[0]));
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

    @Override
    public void seekTo(KeyValue kv) throws IOException {
      queue.clear();
      for (SeekIter<KeyValue> it : iters) {
        it.seekTo(kv);
        if (it.hasNext()) {
          // Only the iterator which has some elements should be enqueued.
          queue.add(new IterNode(it.next(), it));
        }
      }
    }
  }
}
