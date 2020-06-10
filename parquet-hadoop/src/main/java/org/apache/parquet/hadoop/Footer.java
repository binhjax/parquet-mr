package org.apache.parquet.hadoop;


import org.apache.hadoop.fs.Path;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

/**
 * Represent the footer for a given file
 */
public class Footer {

  private final Path file;

  private final ParquetMetadata parquetMetadata;

  public Footer(Path file, ParquetMetadata parquetMetadata) {
    super();
    this.file = file;
    this.parquetMetadata = parquetMetadata;
  }

  public Path getFile() {
    return file;
  }

  public ParquetMetadata getParquetMetadata() {
    return parquetMetadata;
  }

  @Override
  public String toString() {
    return "Footer{"+file+", "+parquetMetadata+"}";
  }
}
