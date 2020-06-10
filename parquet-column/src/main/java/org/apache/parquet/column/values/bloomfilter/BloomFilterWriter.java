package org.apache.parquet.column.values.bloomfilter;

public interface BloomFilterWriter {
  /**
   * Write a Bloom filter
   *
   * @param bloomFilter the Bloom filter to write
   *
   */
  void writeBloomFilter(BloomFilter bloomFilter);
}
