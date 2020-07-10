package com.teko.parquet;
import com.bettercloud.vault.SslConfig;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import com.bettercloud.vault.response.LogicalResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.ColumnDecryptionProperties;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.crypto.DecryptionKeyRetriever;

public class TekoPrestoFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {
  public static String KMS_INSTANCE_URL = "encryption.kms.instance.url";
  public static String KMS_USERNAME = "encryption.user.username";
  public static String KMS_PASSWORD = "encryption.user.password";
  public static String KMS_CURRENT = "encryption.user.current";

  private static byte[] DEFAULT_FOOTER_KEY = {
    0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10
  };

  Map<String, String> keyList = new HashMap<String, String>();
  Map<String, String> columnKeys = new HashMap<String, String>();
  String footerKey = null;


  // Simple key retriever, based on UTF8 strings as key identifiers
  static class StringKeyIdRetriever implements DecryptionKeyRetriever {

    private final Hashtable<String, byte[]> keyMap = new Hashtable<String, byte[]>();

    public void putKey(String keyId, byte[] keyBytes) {
      keyMap.put(keyId, keyBytes);
    }

    @Override
    public byte[] getKey(byte[] keyMetaData) {
      String keyId = new String(keyMetaData, StandardCharsets.UTF_8);
      return keyMap.get(keyId);
    }
  }


  static class VaultKeyIdRetriever implements DecryptionKeyRetriever {

    String vault_url = null;
    String access_token = null;

    public VaultKeyIdRetriever(String vault_url, String access_token) {
        this.vault_url = vault_url;
        this.access_token = access_token;
    }

    @Override
    public byte[] getKey(byte[] keyMetaData) {
      String keyPath_keyId = new String(keyMetaData, StandardCharsets.UTF_8);
      System.out.println("TekoVaultFactory.getKey: keyPath_keyId = " + keyPath_keyId);
      int p = keyPath_keyId.indexOf('|');
      String keyPath = "";
      String keyId = "";
      if (p >= 0) {
         keyPath = keyPath_keyId.substring(0, p);
         keyId = keyPath_keyId.substring(p + 1);
      }

      System.out.println("TekoVaultFactory.getKey: keyPath = " + keyPath + ", keyId= " + keyId);

      String column_key = "";
      //Query to vault to get key
      try {
        VaultConfig config =
            new VaultConfig()
                .address(this.vault_url)
                .token(this.access_token)
                .sslConfig(new SslConfig().verify(false).build())
                .build();
        Vault vault = new Vault(config, 2);

        Map<String, String> vault_data = vault.logical().read(keyPath).getData();
        System.out.println(vault_data);
        if (vault_data.containsKey(keyId)){
            column_key = vault_data.get(keyId);
        }
        System.out.println(column_key);
      } catch (VaultException e) {
        System.out.println("Cannot create config ." + e.toString());
      }
      return column_key.getBytes(StandardCharsets.UTF_8);
    }
  }

  public String generateKey() {
    Random random = ThreadLocalRandom.current();
    byte[] randomBytes = new byte[16];
    random.nextBytes(randomBytes);
    String encoded = Base64.getUrlEncoder().encodeToString(randomBytes);
    return encoded;
  }

  @Override
  public FileEncryptionProperties getFileEncryptionProperties(
      Configuration fileHadoopConfig, Path tempFilePath, WriteSupport.WriteContext fileWriteContext)
      throws ParquetCryptoRuntimeException {

    // loadConfiguration(fileHadoopConfig);
    Map<ColumnPath, ColumnEncryptionProperties> columnEncPropertiesMap = new HashMap<>();

    // for (String keyId : columnKeys.keySet()) {
    //   String columnName = columnKeys.get(keyId);
    //   String columnKey = keyList.get(keyId);
    //   System.out.println(
    //       "binhnt.TekoPropertiesFactory: getFileEncryptionProperties columnName = "
    //           + columnName
    //           + ", columnKey = "
    //           + columnKey);
    //
    //   ColumnPath COL = ColumnPath.fromDotString(columnName);
    //
    //   byte[] COL_KEY = columnKey.getBytes();
    //   ColumnEncryptionProperties COL_ENCR_PROPERTIES =
    //       ColumnEncryptionProperties.builder(COL.toDotString())
    //           .withKey(COL_KEY)
    //           .withKeyID(keyId)
    //           .build();
    //   columnEncPropertiesMap.put(COL, COL_ENCR_PROPERTIES);
    // }
    // System.out.println(
    //     "binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey = " + footerKey);
    //
    // String footerKey_str = keyList.get(footerKey);
    String footerKey_str = null;
    byte[] FOOTER_KEY = null;

    if (footerKey_str != null) {
      System.out.println(
          "binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey_str = "
              + footerKey_str);
      FOOTER_KEY = footerKey_str.getBytes();
      FileEncryptionProperties.Builder fileEncBuilder =
          FileEncryptionProperties.builder(FOOTER_KEY);
      return fileEncBuilder
          .withAlgorithm(ParquetCipher.AES_GCM_V1)
          .withEncryptedColumns(columnEncPropertiesMap)
          .build();
    } else {
      System.out.println(
          "binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey_str is null => withPlaintextFooter");
      FileEncryptionProperties.Builder fileEncBuilder =
          FileEncryptionProperties.builder(DEFAULT_FOOTER_KEY);
      return fileEncBuilder
          .withAlgorithm(ParquetCipher.AES_GCM_V1)
          .withEncryptedColumns(columnEncPropertiesMap)
          .withPlaintextFooter()
          .build();
    }
  }

  @Override
  public FileDecryptionProperties getFileDecryptionProperties(
      Configuration hadoopConfig, Path filePath) throws ParquetCryptoRuntimeException {

    System.out.println("TekoPrestoFactory.getFileDecryptionProperties: filePath = " + filePath.toString());
    String path = filePath.toString();
    // String lpath = path.substring(0, path.lastIndexOf('/'));
    // String secrets_path = lpath.replace("s3a:/", "kv/parquet");

    //1. Login to get access token
    String vault_url = hadoopConfig.get(KMS_INSTANCE_URL);
    String username = hadoopConfig.get(KMS_USERNAME);
    String password = hadoopConfig.get(KMS_PASSWORD);
    String user = hadoopConfig.get(KMS_CURRENT);

    System.out.println("Current user access presto: " + user);
    String access_token = null;
    try {
        VaultConfig config =
            new VaultConfig()
                .address(vault_url)
                .sslConfig(new SslConfig().verify(false).build())
                .build();

        Vault vault = new Vault(config, 2);

        AuthResponse response = vault.auth().loginByUserPass(username, password);

        access_token = response.getAuthClientToken();
        // System.out.println("TekoPrestoFactory.getFileDecryptionProperties: secrets_path = " + secrets_path + ", access_token = " + access_token);

    } catch (VaultException e) {
      System.out.println("Binhnt.loadEncryptConfiguration: Cannot get access token from config " + e.toString());
    }

    VaultKeyIdRetriever kr1 = new VaultKeyIdRetriever(vault_url,access_token);

    // //2. Login to get column cryto config
    // Map<String, String> columns = new HashMap<String, String>();
    // try {
    //   VaultConfig config =
    //       new VaultConfig()
    //           .address(vault_url)
    //           .token(access_token)
    //           .sslConfig(new SslConfig().verify(false).build())
    //           .build();
    //   Vault vault = new Vault(config, 2);
    //   columns = vault.logical().read(secrets_path).getData();
    // } catch (VaultException e) {
    //    System.out.println("Binhnt.loadEncryptConfiguration:  Cannot get column config from config ." + e.toString());
    // }
    //
    // //3. Display info
    // for (Map.Entry<String, String> entry : columns.entrySet()) {
    //   System.out.println("Binhnt: Key = " + entry.getKey() + ", Value = " + entry.getValue());
    // }
    //
    // //4. Initialize retriever
    // StringKeyIdRetriever kr1 = new StringKeyIdRetriever();
    // for (String columnName : columns.keySet()) {
    //   System.out.println("binhnt.TekoPrestoFactory: getFileDecryptionProperties:   columnName = "+ columnName);
    //   String columnKey = columns.get(columnName);
    //   kr1.putKey(columnName, columnKey.getBytes());
    // }

    FileDecryptionProperties.Builder fileDecBuilder = FileDecryptionProperties.builder();

    // String footerKey = columns.get("footer_key");
    // if (footerKey != null) {
      // return fileDecBuilder.withKeyRetriever(kr1).build();
    // } else {
    return fileDecBuilder.withKeyRetriever(kr1).withoutFooterSignatureVerification().build();
    // }
  }
}
