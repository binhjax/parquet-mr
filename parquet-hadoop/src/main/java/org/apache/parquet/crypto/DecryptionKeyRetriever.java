package org.apache.parquet.crypto;

/**
 * Interface for classes retrieving encryption keys using the key metadata. Implementations must be
 * thread-safe, if same KeyRetriever object is passed to multiple file readers.
 */
public interface DecryptionKeyRetriever {

  /**
   * Returns encryption key using the key metadata. If your key retrieval code throws runtime
   * exceptions related to access control (permission) problems (such as Hadoop
   * AccessControlException), catch them and throw the KeyAccessDeniedException.
   *
   * @param keyMetaData arbitrary byte array with encryption key metadata
   * @return encryption key. Key length can be either 16, 24 or 32 bytes.
   * @throws KeyAccessDeniedException thrown upon access control problems (authentication or
   *     authorization)
   * @throws ParquetCryptoRuntimeException thrown upon key retrieval problems unrelated to access
   *     control
   */
  public byte[] getKey(byte[] keyMetaData)
      throws KeyAccessDeniedException, ParquetCryptoRuntimeException;
}
