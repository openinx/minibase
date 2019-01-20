package org.apache.minibase;

public class Config {

  private long maxMemstoreSize = 16 * 1024 * 1024;
  private String dataDir = "MiniBase";
  private int maxDiskFiles = 10;

  private static final Config DEFAULT = new Config();

  public Config setMaxMemstoreSize(long maxMemstoreSize) {
    this.maxMemstoreSize = maxMemstoreSize;
    return this;
  }

  public long getMaxMemstoreSize() {
    return this.maxMemstoreSize;
  }

  public Config setDataDir(String dataDir) {
    this.dataDir = dataDir;
    return this;
  }

  public String getDataDir() {
    return this.dataDir;
  }

  public Config setMaxDiskFiles(int maxDiskFiles) {
    this.maxMemstoreSize = maxDiskFiles;
    return this;
  }

  public int getMaxDiskFiles() {
    return this.maxDiskFiles;
  }

  public static Config getDefault() {
    return DEFAULT;
  }
}
