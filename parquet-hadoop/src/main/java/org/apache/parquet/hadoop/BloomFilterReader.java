package org.apache.parquet.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bloom filter reader that reads Bloom filter data from an open {@link ParquetFileReader}.
 */
public class BloomFilterReader {
  private final ParquetFileReader reader;
  private final Map<ColumnPath, ColumnChunkMetaData> columns;
  private final Map<ColumnPath, BloomFilter> cache = new HashMap<>();
  private Logger logger = LoggerFactory.getLogger(BloomFilterReader.class);

  public BloomFilterReader(ParquetFileReader fileReader, BlockMetaData block) {
    this.reader = fileReader;
    this.columns = new HashMap<>();
    for (ColumnChunkMetaData column : block.getColumns()) {
      columns.put(column.getPath(), column);
    }
  }

  public BloomFilter readBloomFilter(ColumnChunkMetaData meta) {
    if (cache.containsKey(meta.getPath())) {
      return cache.get(meta.getPath());
    }
    try {
      if (!cache.containsKey(meta.getPath())) {
        BloomFilter bloomFilter = reader.readBloomFilter(meta);
        if (bloomFilter == null) {
          return null;
        }

        cache.put(meta.getPath(), bloomFilter);
      }
      return cache.get(meta.getPath());
    } catch (IOException e) {
      logger.error("Failed to read Bloom filter data", e);
    }

    return null;
  }
}
