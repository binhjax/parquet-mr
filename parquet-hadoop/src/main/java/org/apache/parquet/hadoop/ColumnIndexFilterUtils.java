package org.apache.parquet.hadoop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/**
 * Internal utility class to help at column index based filtering.
 */
class ColumnIndexFilterUtils {
  static class OffsetRange {
    private final long offset;
    private long length;

    private OffsetRange(long offset, int length) {
      this.offset = offset;
      this.length = length;
    }

    long getOffset() {
      return offset;
    }

    long getLength() {
      return length;
    }

    private boolean extend(long offset, int length) {
      if (this.offset + this.length == offset) {
        this.length += length;
        return true;
      } else {
        return false;
      }
    }
  }

  private static class FilteredOffsetIndex implements OffsetIndex {
    private final OffsetIndex offsetIndex;
    private final int[] indexMap;

    private FilteredOffsetIndex(OffsetIndex offsetIndex, int[] indexMap) {
      this.offsetIndex = offsetIndex;
      this.indexMap = indexMap;
    }

    @Override
    public int getPageOrdinal(int pageIndex) {
      return indexMap[pageIndex];
    }

    @Override
    public int getPageCount() {
      return indexMap.length;
    }

    @Override
    public long getOffset(int pageIndex) {
      return offsetIndex.getOffset(indexMap[pageIndex]);
    }

    @Override
    public int getCompressedPageSize(int pageIndex) {
      return offsetIndex.getCompressedPageSize(indexMap[pageIndex]);
    }

    @Override
    public long getFirstRowIndex(int pageIndex) {
      return offsetIndex.getFirstRowIndex(indexMap[pageIndex]);
    }

    @Override
    public long getLastRowIndex(int pageIndex, long totalRowCount) {
      int nextIndex = indexMap[pageIndex] + 1;
      return (nextIndex >= offsetIndex.getPageCount() ? totalRowCount : offsetIndex.getFirstRowIndex(nextIndex)) - 1;
    }

    @Override
    public String toString() {
      try (Formatter formatter = new Formatter()) {
        formatter.format("%-12s  %20s  %16s  %20s\n", "", "offset", "compressed size", "first row index");
        for (int i = 0, n = offsetIndex.getPageCount(); i < n; ++i) {
          int index = Arrays.binarySearch(indexMap, i);
          boolean isHidden = index < 0;
          formatter.format("%spage-%-5d  %20d  %16d  %20d\n",
              isHidden ? "- " : "  ",
              isHidden ? i : index,
              offsetIndex.getOffset(i),
              offsetIndex.getCompressedPageSize(i),
              offsetIndex.getFirstRowIndex(i));
        }
        return formatter.toString();
      }
    }
  }

  /*
   * Returns the filtered offset index containing only the pages which are overlapping with rowRanges.
   */
  static OffsetIndex filterOffsetIndex(OffsetIndex offsetIndex, RowRanges rowRanges, long totalRowCount) {
    IntList indexMap = new IntArrayList();
    for (int i = 0, n = offsetIndex.getPageCount(); i < n; ++i) {
      long from = offsetIndex.getFirstRowIndex(i);
      if (rowRanges.isOverlapping(from, offsetIndex.getLastRowIndex(i, totalRowCount))) {
        indexMap.add(i);
      }
    }
    return new FilteredOffsetIndex(offsetIndex, indexMap.toIntArray());
  }

  static List<OffsetRange> calculateOffsetRanges(OffsetIndex offsetIndex, ColumnChunkMetaData cm,
      long firstPageOffset) {
    List<OffsetRange> ranges = new ArrayList<>();
    int n = offsetIndex.getPageCount();
    if (n > 0) {
      OffsetRange currentRange = null;

      // Add a range for the dictionary page if required
      long rowGroupOffset = cm.getStartingPos();
      if (rowGroupOffset < firstPageOffset) {
        currentRange = new OffsetRange(rowGroupOffset, (int) (firstPageOffset - rowGroupOffset));
        ranges.add(currentRange);
      }

      for (int i = 0; i < n; ++i) {
        long offset = offsetIndex.getOffset(i);
        int length = offsetIndex.getCompressedPageSize(i);
        if (currentRange == null || !currentRange.extend(offset, length)) {
          currentRange = new OffsetRange(offset, length);
          ranges.add(currentRange);
        }
      }
    }
    return ranges;
  }
}
