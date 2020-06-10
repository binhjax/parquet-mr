package org.apache.parquet.internal.hadoop.metadata;

/**
 * Reference to an index (OffsetIndex and ColumnIndex) for a row-group containing the offset and length values so the
 * reader can read the referenced data.
 */
public class IndexReference {
  private final long offset;
  private final int length;

  public IndexReference(long offset, int length) {
    this.offset = offset;
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }
}
