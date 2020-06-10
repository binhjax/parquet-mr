package org.apache.parquet.format;

import java.io.IOException;
import java.io.InputStream;

public interface BlockCipher{


  public interface Encryptor{
    /**
     * Encrypts the plaintext.
     *
     * @param plaintext - starts at offset 0 of the input, and fills up the entire byte array.
     * @param AAD - Additional Authenticated Data for the encryption (ignored in case of CTR cipher)
     * @return lengthAndCiphertext The first 4 bytes of the returned value are the ciphertext length (little endian int).
     * The ciphertext starts at offset 4  and fills up the rest of the returned byte array.
     * The ciphertext includes the nonce and (in case of GCM cipher) the tag, as detailed in the
     * Parquet Modular Encryption specification.
     */
    public byte[] encrypt(byte[] plaintext, byte[] AAD);
  }


  public interface Decryptor{
    /**
     * Decrypts the ciphertext.
     *
     * @param lengthAndCiphertext - The first 4 bytes of the input are the ciphertext length (little endian int).
     * The ciphertext starts at offset 4  and fills up the rest of the input byte array.
     * The ciphertext includes the nonce and (in case of GCM cipher) the tag, as detailed in the
     * Parquet Modular Encryption specification.
     * @param AAD - Additional Authenticated Data for the decryption (ignored in case of CTR cipher)
     * @return plaintext - starts at offset 0 of the output value, and fills up the entire byte array.
     */
    public byte[] decrypt(byte[] lengthAndCiphertext, byte[] AAD);

    /**
     * Convenience decryption method that reads the length and ciphertext from the input stream.
     *
     * @param from Input stream with length and ciphertext.
     * @param AAD - Additional Authenticated Data for the decryption (ignored in case of CTR cipher)
     * @return plaintext -  starts at offset 0 of the output, and fills up the entire byte array.
     * @throws IOException - Stream I/O problems
     */
    public byte[] decrypt(InputStream from, byte[] AAD) throws IOException;
  }
}
