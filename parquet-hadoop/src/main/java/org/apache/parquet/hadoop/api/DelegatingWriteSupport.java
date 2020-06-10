package org.apache.parquet.hadoop.api;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.io.api.RecordConsumer;

/**
 *
 * Helps composing write supports
 *
 * @param <T> the Java class of objects written with this WriteSupport
 */
public class DelegatingWriteSupport<T> extends WriteSupport<T> {

  private final WriteSupport<T> delegate;

  public DelegatingWriteSupport(WriteSupport<T> delegate) {
    super();
    this.delegate = delegate;
  }

  @Override
  public WriteSupport.WriteContext init(Configuration configuration) {
    return delegate.init(configuration);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    delegate.prepareForWrite(recordConsumer);
  }

  @Override
  public void write(T record) {
    delegate.write(record);
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    return delegate.finalizeWrite();
  }

  @Override
  public String toString() {
    return getClass().getName() + "(" + delegate.toString() + ")";
  }
}
