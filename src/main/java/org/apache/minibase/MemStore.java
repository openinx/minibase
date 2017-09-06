package org.apache.minibase;

import java.util.concurrent.ConcurrentSkipListMap;

public class MemStore {

  private ConcurrentSkipListMap<byte[], KeyValue> skiplist =
      new ConcurrentSkipListMap<byte[], KeyValue>();

}
