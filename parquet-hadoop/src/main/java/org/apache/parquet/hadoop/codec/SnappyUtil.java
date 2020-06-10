package org.apache.parquet.hadoop.codec;

import org.apache.parquet.Preconditions;

/**
 * Utilities for SnappyCompressor and SnappyDecompressor.
 */
public class SnappyUtil {
  public static void validateBuffer(byte[] buffer, int off, int len) {
    Preconditions.checkNotNull(buffer, "buffer");
    Preconditions.checkArgument(off >= 0 && len >= 0 && off <= buffer.length - len,
        "Invalid buffer offset or length: buffer.length=%s off=%s len=%s",
        buffer.length, off, len);
  }
}
