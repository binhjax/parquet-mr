package org.apache.parquet.hadoop.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Block metadata stored in the footer and passed in an InputSplit
 */
public class BlockMetaData {

  private List<ColumnChunkMetaData> columns = new ArrayList<ColumnChunkMetaData>();
  private long rowCount;
  private long totalByteSize;
  private String path;
  private int ordinal;

  public BlockMetaData() {
  }

  /**
   * @param path the path to the file containing the data. Or null if same file the metadata was found
   */
  public void setPath(String path) {
    this.path = path;
  }

  /**
   * @return the path relative to the parent of this file where the data is. Or null if it is in the same file.
   */
  public String getPath() {
    return path;
  }

  /**
   * @return the rowCount
   */
  public long getRowCount() {
    return rowCount;
  }

  /**
   * @param rowCount the rowCount to set
   */
  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  /**
   * @return the totalByteSize
   */
  public long getTotalByteSize() {
    return totalByteSize;
  }

  /**
   * @param totalByteSize the totalByteSize to set
   */
  public void setTotalByteSize(long totalByteSize) {
    this.totalByteSize = totalByteSize;
  }

  /**
   *
   * @param column the metadata for a column
   */
  public void addColumn(ColumnChunkMetaData column) {
    columns.add(column);
  }

  /**
   *
   * @return the metadata for columns
   */
  public List<ColumnChunkMetaData> getColumns() {
    return Collections.unmodifiableList(columns);
  }

  /**
   *
   * @return the starting pos of first column
   */
  public long getStartingPos() {
    return getColumns().get(0).getStartingPos();
  }

  @Override
  public String toString() {
    return "BlockMetaData{" + rowCount + ", " + totalByteSize + " " + columns + "}";
  }

  /**
   * @return the compressed size of all columns
   */
  public long getCompressedSize() {
    long totalSize = 0;
    for (ColumnChunkMetaData col : getColumns()) {
      totalSize += col.getTotalSize();
    }
    return totalSize;
  }

  /**
   * @return row group ordinal
   */
  public int getOrdinal() {
    return ordinal;
  }

  /**
  *
  * @param ordinal - row group ordinal
  */
  public void setOrdinal(int ordinal) {
    this.ordinal = ordinal;
  }
}
