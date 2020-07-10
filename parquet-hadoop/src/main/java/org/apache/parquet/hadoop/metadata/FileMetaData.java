package org.apache.parquet.hadoop.metadata;

import static java.util.Collections.unmodifiableMap;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.schema.MessageType;

/** File level meta data (Schema, codec, ...) */
public final class FileMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private final MessageType schema;

  private final Map<String, String> keyValueMetaData;

  private final String createdBy;

  private final InternalFileDecryptor fileDecryptor;

  /**
   * @param schema the schema for the file
   * @param keyValueMetaData the app specific metadata
   * @param createdBy the description of the library that created the file
   * @throws NullPointerException if schema or keyValueMetaData is {@code null}
   */
  public FileMetaData(MessageType schema, Map<String, String> keyValueMetaData, String createdBy) {
    this(schema, keyValueMetaData, createdBy, null);
    System.out.println("FileMetaData.construct: start 1 ");
  }

  public FileMetaData(
      MessageType schema,
      Map<String, String> keyValueMetaData,
      String createdBy,
      InternalFileDecryptor fileDecryptor) {
    super();
    System.out.println("FileMetaData.construct: start 2 ");

    this.schema = Objects.requireNonNull(schema, "schema cannot be null");
    this.keyValueMetaData =
        unmodifiableMap(
            Objects.requireNonNull(keyValueMetaData, "keyValueMetaData cannot be null"));
    this.createdBy = createdBy;
    this.fileDecryptor = fileDecryptor;
  }

  /** @return the schema for the file */
  public MessageType getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return "FileMetaData{schema: " + schema + ", metadata: " + keyValueMetaData + "}";
  }

  /** @return meta data for extensions */
  public Map<String, String> getKeyValueMetaData() {
    return keyValueMetaData;
  }

  /** @return the description of the library that created the file */
  public String getCreatedBy() {
    return createdBy;
  }

  public InternalFileDecryptor getFileDecryptor() {
    return fileDecryptor;
  }
}
