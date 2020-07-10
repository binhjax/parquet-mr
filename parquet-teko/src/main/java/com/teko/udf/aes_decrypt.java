package com.teko.udf;

import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;
import org.apache.spark.sql.api.java.UDF2;

public class aes_decrypt implements UDF2<String, String, String> {
  @Override
  public String call(String passwordhex, String strKey) throws Exception {
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
}
