package com.teko;

import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;

public class TekoAESCrypto {
  public static String Decrypt(String passwordhex, String strKey) throws Exception {
    try {
      byte[] keyBytes = Arrays.copyOf(strKey.getBytes("ASCII"), 16);

      SecretKey key = new SecretKeySpec(keyBytes, "AES");
      Cipher decipher = Cipher.getInstance("AES");

      decipher.init(Cipher.DECRYPT_MODE, key);

      char[] cleartext = passwordhex.toCharArray();

      byte[] decodeHex = Hex.decodeHex(cleartext);

      byte[] ciphertextBytes = decipher.doFinal(decodeHex);

      return new String(ciphertextBytes);

    } catch (Exception e) {
      e.getMessage();
    }
    return null;
  }

  public static String Encrypt(String password, String strKey) throws Exception {
    try {
      byte[] keyBytes = Arrays.copyOf(strKey.getBytes("ASCII"), 16);

      SecretKey key = new SecretKeySpec(keyBytes, "AES");
      Cipher cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.ENCRYPT_MODE, key);

      byte[] cleartext = password.getBytes("UTF-8");
      byte[] ciphertextBytes = cipher.doFinal(cleartext);

      return new String(Hex.encodeHex(ciphertextBytes));

    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
}
