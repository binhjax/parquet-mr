package org.apache.parquet.crypto;

import java.util.HashMap;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.EncryptionAlgorithm;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

public class InternalFileEncryptor {

  private final EncryptionAlgorithm algorithm;
  private final FileEncryptionProperties fileEncryptionProperties;
  private final byte[] footerKey;
  private final byte[] footerKeyMetadata;
  private final HashMap<ColumnPath, InternalColumnEncryptionSetup> columnMap;
  private final byte[] fileAAD;
  private final boolean encryptFooter;

  private BlockCipher.Encryptor aesGcmEncryptorWithFooterKey;
  private BlockCipher.Encryptor aesCtrEncryptorWithFooterKey;
  private boolean fileCryptoMetaDataCreated;

  public InternalFileEncryptor(FileEncryptionProperties fileEncryptionProperties) {
    this.fileEncryptionProperties = fileEncryptionProperties;
    algorithm = fileEncryptionProperties.getAlgorithm();
    footerKey = fileEncryptionProperties.getFooterKey();
    encryptFooter = fileEncryptionProperties.encryptedFooter();
    footerKeyMetadata = fileEncryptionProperties.getFooterKeyMetadata();
    fileAAD = fileEncryptionProperties.getFileAAD();
    columnMap = new HashMap<ColumnPath, InternalColumnEncryptionSetup>();
    fileCryptoMetaDataCreated = false;
  }

  private BlockCipher.Encryptor getThriftModuleEncryptor(byte[] columnKey) {
    if (null == columnKey) { // Encryptor with footer key
      if (null == aesGcmEncryptorWithFooterKey) {
        aesGcmEncryptorWithFooterKey = ModuleCipherFactory.getEncryptor(AesMode.GCM, footerKey);
      }
      return aesGcmEncryptorWithFooterKey;
    } else { // Encryptor with column key
      return ModuleCipherFactory.getEncryptor(AesMode.GCM, columnKey);
    }
  }

  private BlockCipher.Encryptor getDataModuleEncryptor(byte[] columnKey) {
    if (algorithm.isSetAES_GCM_V1()) {
      return getThriftModuleEncryptor(columnKey);
    }
    // AES_GCM_CTR_V1
    if (null == columnKey) { // Encryptor with footer key
      if (null == aesCtrEncryptorWithFooterKey) {
        aesCtrEncryptorWithFooterKey = ModuleCipherFactory.getEncryptor(AesMode.CTR, footerKey);
      }
      return aesCtrEncryptorWithFooterKey;
    } else { // Encryptor with column key
      return ModuleCipherFactory.getEncryptor(AesMode.CTR, columnKey);
    }
  }

  public InternalColumnEncryptionSetup getColumnSetup(
      ColumnPath columnPath, boolean createIfNull, int ordinal) {
    InternalColumnEncryptionSetup internalColumnProperties = columnMap.get(columnPath);

    if (null != internalColumnProperties) {
      if (ordinal != internalColumnProperties.getOrdinal()) {
        throw new ParquetCryptoRuntimeException(
            "Column ordinal doesnt match "
                + columnPath
                + ": "
                + ordinal
                + ", "
                + internalColumnProperties.getOrdinal());
      }
      return internalColumnProperties;
    }

    if (!createIfNull) {
      throw new ParquetCryptoRuntimeException("No encryption setup found for column " + columnPath);
    }
    if (fileCryptoMetaDataCreated) {
      throw new ParquetCryptoRuntimeException(
          "Re-use: No encryption setup for column " + columnPath);
    }

    ColumnEncryptionProperties columnProperties =
        fileEncryptionProperties.getColumnProperties(columnPath);
    if (null == columnProperties) {
      throw new ParquetCryptoRuntimeException("No encryption properties for column " + columnPath);
    }
    if (columnProperties.isEncrypted()) {
      if (columnProperties.isEncryptedWithFooterKey()) {
        internalColumnProperties =
            new InternalColumnEncryptionSetup(
                columnProperties,
                ordinal,
                getDataModuleEncryptor(null),
                getThriftModuleEncryptor(null));
      } else {
        internalColumnProperties =
            new InternalColumnEncryptionSetup(
                columnProperties,
                ordinal,
                getDataModuleEncryptor(columnProperties.getKeyBytes()),
                getThriftModuleEncryptor(columnProperties.getKeyBytes()));
      }
    } else { // unencrypted column
      internalColumnProperties =
          new InternalColumnEncryptionSetup(columnProperties, ordinal, null, null);
    }
    columnMap.put(columnPath, internalColumnProperties);

    return internalColumnProperties;
  }

  public BlockCipher.Encryptor getFooterEncryptor() {
    if (!encryptFooter) return null;
    return getThriftModuleEncryptor(null);
  }

  public FileCryptoMetaData getFileCryptoMetaData() {
    if (!encryptFooter) {
      throw new ParquetCryptoRuntimeException(
          "Requesting FileCryptoMetaData in file with unencrypted footer");
    }
    FileCryptoMetaData fileCryptoMetaData = new FileCryptoMetaData(algorithm);
    if (null != footerKeyMetadata) {
      fileCryptoMetaData.setKey_metadata(footerKeyMetadata);
    }
    fileCryptoMetaDataCreated = true;

    return fileCryptoMetaData;
  }

  public boolean encryptColumnMetaData(InternalColumnEncryptionSetup columnSetup) {
    if (!columnSetup.isEncrypted()) {
      return false;
    }
    if (!encryptFooter) {
      return true;
    }

    return !columnSetup.isEncryptedWithFooterKey();
  }

  public boolean isFooterEncrypted() {
    return encryptFooter;
  }

  public EncryptionAlgorithm getEncryptionAlgorithm() {
    return algorithm;
  }

  public byte[] getFileAAD() {
    return this.fileAAD;
  }

  public byte[] getFooterSigningKeyMetaData() {
    if (encryptFooter) {
      throw new ParquetCryptoRuntimeException(
          "Requesting signing footer key metadata in file with encrypted footer");
    }
    return footerKeyMetadata;
  }

  public AesGcmEncryptor getSignedFooterEncryptor() {
    if (encryptFooter) {
      throw new ParquetCryptoRuntimeException(
          "Requesting signed footer encryptor in file with encrypted footer");
    }
    return (AesGcmEncryptor) ModuleCipherFactory.getEncryptor(AesMode.GCM, footerKey);
  }
}
