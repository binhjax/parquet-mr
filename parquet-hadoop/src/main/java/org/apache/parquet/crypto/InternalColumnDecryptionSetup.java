package org.apache.parquet.crypto;

import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.hadoop.metadata.ColumnPath;

public class InternalColumnDecryptionSetup {

  private final ColumnPath columnPath;
  private final boolean isEncrypted;
  private final boolean isEncryptedWithFooterKey;
  private final BlockCipher.Decryptor dataDecryptor;
  private final BlockCipher.Decryptor metaDataDecryptor;
  private final int columnOrdinal;
  private final byte[] keyMetadata;

  InternalColumnDecryptionSetup(
      ColumnPath path,
      boolean encrypted,
      boolean isEncryptedWithFooterKey,
      BlockCipher.Decryptor dataDecryptor,
      BlockCipher.Decryptor metaDataDecryptor,
      int columnOrdinal,
      byte[] keyMetadata) {
    this.columnPath = path;
    this.isEncrypted = encrypted;
    this.isEncryptedWithFooterKey = isEncryptedWithFooterKey;
    this.dataDecryptor = dataDecryptor;
    this.metaDataDecryptor = metaDataDecryptor;
    this.columnOrdinal = columnOrdinal;
    this.keyMetadata = keyMetadata;
  }

  public boolean isEncrypted() {
    return isEncrypted;
  }

  public BlockCipher.Decryptor getDataDecryptor() {
    return dataDecryptor;
  }

  public BlockCipher.Decryptor getMetaDataDecryptor() {
    return metaDataDecryptor;
  }

  boolean isEncryptedWithFooterKey() {
    return isEncryptedWithFooterKey;
  }

  ColumnPath getPath() {
    return columnPath;
  }

  public int getOrdinal() {
    return columnOrdinal;
  }

  byte[] getKeyMetadata() {
    return keyMetadata;
  }
}
