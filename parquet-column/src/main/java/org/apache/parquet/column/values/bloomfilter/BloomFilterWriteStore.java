package org.apache.parquet.column.values.bloomfilter;

import org.apache.parquet.column.ColumnDescriptor;

/**
 * Contains all writers for all columns of a row group
 */
public interface BloomFilterWriteStore {
  /**
   * Get bloom filter writer of a column
   *
   * @param path the descriptor for the column
   * @return the corresponding Bloom filter writer
   */
  BloomFilterWriter getBloomFilterWriter(ColumnDescriptor path);
}
