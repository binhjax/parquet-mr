package org.apache.parquet.hadoop.metadata;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.parquet.column.Encoding;

public class EncodingList implements Iterable<Encoding> {

  private static Canonicalizer<EncodingList> encodingLists = new Canonicalizer<EncodingList>();

  public static EncodingList getEncodingList(List<Encoding> encodings) {
    return encodingLists.canonicalize(new EncodingList(encodings));
  }

  private final List<Encoding> encodings;

  private EncodingList(List<Encoding> encodings) {
    super();
    this.encodings = Collections.unmodifiableList(encodings);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof EncodingList) {
      List<org.apache.parquet.column.Encoding> other = ((EncodingList)obj).encodings;
      final int size = other.size();
      if (size != encodings.size()) {
        return false;
      }
      for (int i = 0; i < size; i++) {
        if (!other.get(i).equals(encodings.get(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 1;
    for (org.apache.parquet.column.Encoding element : encodings)
      result = 31 * result + (element == null ? 0 : element.hashCode());
    return result;
  }

  public List<Encoding> toList() {
    return encodings;
  }

  @Override
  public Iterator<Encoding> iterator() {
    return encodings.iterator();
  }

  public int size() {
    return encodings.size();
  }

}
