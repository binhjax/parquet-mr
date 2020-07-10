package org.apache.parquet.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import org.apache.parquet.format.BlockCipher;

public class AesGcmDecryptor extends AesCipher implements BlockCipher.Decryptor {

  AesGcmDecryptor(byte[] keyBytes) {
    super(AesMode.GCM, keyBytes);
    System.out.println("AesGcmDecryptor.construct: start ");
    System.out.println(Arrays.toString(keyBytes));

    try {
      cipher = Cipher.getInstance(AesMode.GCM.getCipherName());
    } catch (GeneralSecurityException e) {
      throw new ParquetCryptoRuntimeException("Failed to create GCM cipher", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] lengthAndCiphertext, byte[] AAD) {
    int cipherTextOffset = SIZE_LENGTH;
    int cipherTextLength = lengthAndCiphertext.length - SIZE_LENGTH;

    return decrypt(lengthAndCiphertext, cipherTextOffset, cipherTextLength, AAD);
  }

  public byte[] decrypt(byte[] ciphertext, int cipherTextOffset, int cipherTextLength, byte[] AAD) {
    System.out.println("AesGcmDecryptor.decrypt: start");
    System.out.println(Arrays.toString(ciphertext));
    System.out.println(Arrays.toString(AAD));
    int plainTextLength = cipherTextLength - GCM_TAG_LENGTH - NONCE_LENGTH;
    if (plainTextLength < 1) {
      throw new ParquetCryptoRuntimeException("Wrong input length " + plainTextLength);
    }

    // Get the nonce from ciphertext
    System.arraycopy(ciphertext, cipherTextOffset, localNonce, 0, NONCE_LENGTH);

    byte[] plainText = new byte[plainTextLength];
    int inputLength = cipherTextLength - NONCE_LENGTH;
    int inputOffset = cipherTextOffset + NONCE_LENGTH;
    int outputOffset = 0;
    try {
      GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, localNonce);
      // System.out.println(localNonce);

      cipher.init(Cipher.DECRYPT_MODE, aesKey, spec);

      if (null != AAD) cipher.updateAAD(AAD);

      cipher.doFinal(ciphertext, inputOffset, inputLength, plainText, outputOffset);
    } catch (AEADBadTagException e) {
      throw new TagVerificationException("GCM tag check failed", e);
    } catch (GeneralSecurityException e) {
      throw new ParquetCryptoRuntimeException("Failed to decrypt", e);
    }

    return plainText;
  }

  @Override
  public byte[] decrypt(InputStream from, byte[] AAD) throws IOException {
    byte[] lengthBuffer = new byte[SIZE_LENGTH];
    int gotBytes = 0;

    // Read the length of encrypted Thrift structure
    while (gotBytes < SIZE_LENGTH) {
      int n = from.read(lengthBuffer, gotBytes, SIZE_LENGTH - gotBytes);
      if (n <= 0) {
        throw new IOException("Tried to read int (4 bytes), but only got " + gotBytes + " bytes.");
      }
      gotBytes += n;
    }

    final int ciphertextLength =
        ((lengthBuffer[3] & 0xff) << 24)
            | ((lengthBuffer[2] & 0xff) << 16)
            | ((lengthBuffer[1] & 0xff) << 8)
            | ((lengthBuffer[0] & 0xff));

    if (ciphertextLength < 1) {
      throw new IOException("Wrong length of encrypted metadata: " + ciphertextLength);
    }

    byte[] ciphertextBuffer = new byte[ciphertextLength];
    gotBytes = 0;
    // Read the encrypted structure contents
    while (gotBytes < ciphertextLength) {
      int n = from.read(ciphertextBuffer, gotBytes, ciphertextLength - gotBytes);
      if (n <= 0) {
        throw new IOException(
            "Tried to read " + ciphertextLength + " bytes, but only got " + gotBytes + " bytes.");
      }
      gotBytes += n;
    }

    // Decrypt the structure contents
    return decrypt(ciphertextBuffer, 0, ciphertextLength, AAD);
  }
}
