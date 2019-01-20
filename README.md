MiniBase is an embedded KV storage engine, it's quit simple, not for production env, just for better understand HBase or
other LSM-related index algorithm.

In MiniBase, we use those basic algorithm and data structure:

* BloomFilter: it can helps a lot when filtering much useless IO.
* ConcurrentSkipListMap: Yeah, it's quite suitable when designing memstore. It can maintian an sorted key value set in
high concurrency scenarios.
* LSM Index Algorithm: the memstore part and disk store part.

### How to use ?

```java
Config conf = new Config().setDataDir(dataDir).setMaxMemstoreSize(1).setFlushMaxRetries(1)
    .setMaxDiskFiles(10);
MiniBase db = MiniBaseImpl.create(conf).open();

// Put
db.put(Bytes.toBytes(1), Bytes.toBytes(1));

// Scan
Iter<KeyValue> kv = db.scan();
while (kv.hasNext()) {
    KeyValue kv = kv.next();
    //...
}
```

### How to build and test?

```shell
git clone git@github.com:openinx/minibase.git
mvn clean package
```
