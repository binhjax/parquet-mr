package com.teko.udf;

import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;
import org.apache.spark.sql.api.java.UDF2;

public class aes_encrypt implements UDF2<String, String, String> {
  @Override
  public String call(String password, String strKey) throws Exception {
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
