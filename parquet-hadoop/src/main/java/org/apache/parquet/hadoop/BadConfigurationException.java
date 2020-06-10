package org.apache.parquet.hadoop;

import org.apache.parquet.ParquetRuntimeException;

/**
 * Thrown when the input/output formats are misconfigured
 */
public class BadConfigurationException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  public BadConfigurationException() {
  }

  public BadConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }

  public BadConfigurationException(String message) {
    super(message);
  }

  public BadConfigurationException(Throwable cause) {
    super(cause);
  }

}
