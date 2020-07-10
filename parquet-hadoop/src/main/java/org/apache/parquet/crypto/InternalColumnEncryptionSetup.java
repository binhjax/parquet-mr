package org.apache.parquet.crypto;

import java.util.Arrays;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.EncryptionWithColumnKey;
import org.apache.parquet.format.EncryptionWithFooterKey;

public class InternalColumnEncryptionSetup {

  private final ColumnEncryptionProperties encryptionProperties;
  private final BlockCipher.Encryptor metadataEncryptor;
  private final BlockCipher.Encryptor dataEncryptor;
  private final ColumnCryptoMetaData columnCryptoMetaData;
  private final int ordinal;

  InternalColumnEncryptionSetup(
      ColumnEncryptionProperties encryptionProperties,
      int ordinal,
      BlockCipher.Encryptor dataEncryptor,
      BlockCipher.Encryptor metaDataEncryptor) {
    this.encryptionProperties = encryptionProperties;
    this.dataEncryptor = dataEncryptor;
    this.metadataEncryptor = metaDataEncryptor;
    this.ordinal = ordinal;

    if (encryptionProperties.isEncrypted()) {
      if (encryptionProperties.isEncryptedWithFooterKey()) {
        columnCryptoMetaData =
            ColumnCryptoMetaData.ENCRYPTION_WITH_FOOTER_KEY(new EncryptionWithFooterKey());
      } else {
        EncryptionWithColumnKey withColumnKeyStruct =
            new EncryptionWithColumnKey(Arrays.asList(encryptionProperties.getPath().toArray()));
        if (null != encryptionProperties.getKeyMetaData()) {
          withColumnKeyStruct.setKey_metadata(encryptionProperties.getKeyMetaData());
        }
        columnCryptoMetaData = ColumnCryptoMetaData.ENCRYPTION_WITH_COLUMN_KEY(withColumnKeyStruct);
      }
    } else {
      columnCryptoMetaData = null;
    }
  }

  public boolean isEncrypted() {
    return encryptionProperties.isEncrypted();
  }

  public BlockCipher.Encryptor getMetaDataEncryptor() {
    return metadataEncryptor;
  }

  public BlockCipher.Encryptor getDataEncryptor() {
    return dataEncryptor;
  }

  public ColumnCryptoMetaData getColumnCryptoMetaData() {
    return columnCryptoMetaData;
  }

  public int getOrdinal() {
    return ordinal;
  }

  public boolean isEncryptedWithFooterKey() {
    return encryptionProperties.isEncryptedWithFooterKey();
  }
}
