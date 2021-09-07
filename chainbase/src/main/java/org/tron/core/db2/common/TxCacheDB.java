package org.tron.core.db2.common;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.primitives.Longs;

import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.sql.Struct;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.WeakHashMap;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.iq80.leveldb.WriteOptions;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.storage.rocksdb.RocksDbDataSourceImpl;
import org.tron.common.utils.StorageUtils;
import org.tron.core.ChainBaseManager;
import org.tron.core.db.common.iterator.DBIterator;

@Slf4j(topic = "DB")
public class TxCacheDB implements DB<byte[], byte[]>, Flusher {

  @Getter
  public static TxCacheDB txCacheDB;

  private volatile int totalGetCount = 0;

  private volatile int bloomFilterCount = 0;

  private volatile int totalPutCount = 0;

  private String name;

  private volatile long beginBlockNum;

  private volatile long count = 0;

  private volatile boolean syncFlag = false;

  private volatile int threshold = 10_000_000;

  BloomFilter filter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()),
          100_000_000);

  BloomFilter filterBak = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()),
          100_000_000);

  private DB<byte[], byte[]> persistentStore;

  public TxCacheDB(String name) {
    txCacheDB = this;

    this.name = name;

    int dbVersion = CommonParameter.getInstance().getStorage().getDbVersion();
    String dbEngine = CommonParameter.getInstance().getStorage().getDbEngine();
    if (dbVersion == 2) {
      if ("LEVELDB".equals(dbEngine.toUpperCase())) {
        this.persistentStore = new LevelDB(
                        new LevelDbDataSourceImpl(StorageUtils.getOutputDirectoryByDbName(name),
                                name, StorageUtils.getOptionsByDbName(name),
                                new WriteOptions().sync(CommonParameter.getInstance()
                                        .getStorage().isDbSync())));
      } else if ("ROCKSDB".equals(dbEngine.toUpperCase())) {
        String parentPath = Paths
                .get(StorageUtils.getOutputDirectoryByDbName(name), CommonParameter
                        .getInstance().getStorage().getDbDirectory()).toString();

        this.persistentStore = new RocksDB(
                        new RocksDbDataSourceImpl(parentPath,
                                name, CommonParameter.getInstance()
                                .getRocksDBCustomSettings()));
      } else {
        throw new RuntimeException("db type is not supported.");
      }
    } else {
      throw new RuntimeException("db version is not supported.");
    }
  }

  public void init(ChainBaseManager chainBaseManager) {
    DBIterator iterator = (DBIterator) persistentStore.iterator();
    while (iterator.hasNext()) {
      Entry<byte[], byte[]> entry = iterator.next();
      byte[] key = entry.getKey();
      filter.put(Hex.encodeHexString(key));
    }
    beginBlockNum = chainBaseManager.getDynamicPropertiesStore().getLatestSolidifiedBlockNum();
  }

  public void setSolidBlockNum(long solidBlockNum) {
    if (!syncFlag && count > threshold && solidBlockNum - beginBlockNum > 70000) {
      beginBlockNum = solidBlockNum;
      syncFlag = true;
      logger.info("### sync flag change to true");
    } else if (syncFlag && solidBlockNum - beginBlockNum > 70000) {
      count = 0;
      syncFlag = false;
      filter = filterBak;
      filterBak = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()),
              100_000_000);
      logger.info("### sync flag change to false");
    }

    if (totalGetCount % 100 == 0) {
      logger.info("#### solidBlockNum:{}, blockCount:{},  totalPutCount:{}, totalGetCount:{}, bloomFilterCount:{}",
              solidBlockNum, solidBlockNum - beginBlockNum, totalPutCount, totalGetCount, bloomFilterCount);
    }

  }

  @Override
  public byte[] get(byte[] key) {
    totalGetCount++;
    if (!filter.mightContain(Hex.encodeHexString(key))) {
      bloomFilterCount++;
      return null;
    }
    return persistentStore.get(key);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    if (key == null || value == null) {
      return;
    }
    totalPutCount++;
    count++;
    filter.put(Hex.encodeHexString(key));
    if (syncFlag) {
      filterBak.put(Hex.encodeHexString(key));
    }
    persistentStore.put(key, value);
  }

  @Override
  public long size() {
    return persistentStore.size();
  }

  @Override
  public boolean isEmpty() {
    return persistentStore.isEmpty();
  }

  @Override
  public void remove(byte[] key) {
    if (key != null) {
      persistentStore.remove(key);
    }
  }

  @Override
  public String getDbName() {
    return name;
  }

  @Override
  public Iterator<Entry<byte[], byte[]>> iterator() {
    return persistentStore.iterator();
  }

  @Override
  public void flush(Map<WrappedByteArray, WrappedByteArray> batch) {
    batch.forEach((k, v) -> this.put(k.getBytes(), v.getBytes()));
  }

  @Override
  public void close() {
    reset();
    persistentStore.close();
  }

  @Override
  public void reset() {}

  @Override
  public TxCacheDB newInstance() {
    return new TxCacheDB(name);
  }
}
