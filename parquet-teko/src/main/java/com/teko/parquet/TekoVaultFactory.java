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
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.parquet.hadoop.ParquetFileReader;

public class TekoVaultFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {
  public static String KMS_INSTANCE_URL = "encryption.kms.instance.url";
  public static String KMS_USERNAME = "encryption.user.username";
  public static String KMS_PASSWORD = "encryption.user.password";
  public static String KMS_SECRET_PATH = "encryption.secrets.path";
  public static String CRYPTO_COLUMNS = "encryption.columns";
  public static String PAQUET_ROW_METADATA = "org.apache.spark.sql.parquet.row.metadata";

  String vault_url = null;

  String username = null;
  String password = null;

  String access_token = null;

  public static TekoVaultFactory instance = null;

  Map<String, Object> secrets = new HashMap<String,Object>();
  Map<String, String> keyIds = new HashMap<String,String>();

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

  public void loadConfiguration(Configuration conf) {
    vault_url = conf.get(KMS_INSTANCE_URL);

    username = conf.get(KMS_USERNAME);
    password = conf.get(KMS_PASSWORD);

    try {
      System.out.println("Binhnt.loadConfiguration: 1. Try to get access token. ");
      VaultConfig config =
          new VaultConfig()
              .address(vault_url)
              .sslConfig(new SslConfig().verify(false).build())
              .build();

      Vault vault = new Vault(config, 2);

      AuthResponse response = vault.auth().loginByUserPass(username, password);

      access_token = response.getAuthClientToken();
      System.out.println(
          "Binhnt.loadEncryptConfiguration:  2. Logged in with username =  "
              + username
              + ", token: "
              + access_token);

    } catch (VaultException e) {
      System.out.println("Binhnt.loadConfiguration: Cannot create config ." + e.toString());
      return;
    }
  }

  // public Dataset secureDataFrame(Dataset df){
  //    System.out.println("TekoVaultFactory.secureDataFrame: start ");
  //
  //    String[] secured_columns = columns.replaceAll("\\s+", "").split(",");
  //    Dataset tmp_df = df;
  //    for(int i=0; i < secured_columns.length; i++){
  //      String secured_column = secured_columns[i];
  //      System.out.println("TekoVaultFactory.secureDataFrame: change column " + secured_column);
  //
  //      Metadata meta = (new MetadataBuilder()).putString("keyPath", secrets_path).putString("keyId", secured_column).build();
  //      tmp_df = tmp_df.withColumn(secured_column,tmp_df.col(secured_column).as(secured_column,meta));
  //    }
  //    return tmp_df;
  // }
  // public static Dataset SecureDataFrame(Dataset df, Configuration conf,String secrets_path, String columns) {
  //   if (instance == null) {
  //     System.out.println("static SecureDataFrame create TekoVaultFactory instance ");
  //     instance = new TekoVaultFactory();
  //   }
  //   instance.secrets_path = secrets_path;
  //   instance.columns = columns;
  //
  //   if (instance.createColumnKeys(conf)){
  //       System.out.println("call  instance.secureDataFrame");
  //       return instance.secureDataFrame(df);
  //   }
  //   return null;
  // }

  public static String InitColumnKey(Configuration conf,String secrets_path, String column) {
    if (instance == null) {
      System.out.println("static SecureDataFrame create TekoVaultFactory instance ");
      instance = new TekoVaultFactory();
    }
    if (instance.createColumnKey(conf,secrets_path,column)){
      Metadata meta = (new MetadataBuilder()).putString("keyPath", secrets_path).putString("keyId", column).build();
      return meta.json();
    }
    return "";
  }

  public boolean createColumnKey(Configuration conf, String secrets_path, String columnName) {
    loadConfiguration(conf);
    System.out.println(
        "Binhnt.loadEncryptConfiguration:  2. Write data to vault:  " + secrets_path);
    try {
      VaultConfig config =
          new VaultConfig()
              .address(vault_url)
              .token(access_token)
              .sslConfig(new SslConfig().verify(false).build())
              .build();

      Vault vault = new Vault(config, 2);
      secrets.put(columnName,generateKey());
      keyIds.put(columnName,secrets_path + "|" + columnName);
      LogicalResponse writeResponse = vault.logical().write(secrets_path, secrets);
      return true;

    } catch (VaultException e) {
      System.out.println("Binhnt.loadEncryptConfiguration: Cannot create config : " + e.toString());
      return false;
    }
  }

  public boolean loadCryptoConfiguration(Configuration conf, Map<String, String> extraMetaData) {
    if (access_token == null) {
      loadConfiguration(conf);
    }
    try {
      VaultConfig config =
          new VaultConfig()
              .address(vault_url)
              .token(access_token)
              .sslConfig(new SslConfig().verify(false).build())
              .build();
      Vault vault = new Vault(config, 2);

      //Get column config
      if (extraMetaData.containsKey(PAQUET_ROW_METADATA)) {
        String rowSchema_json = extraMetaData.get(PAQUET_ROW_METADATA);

        StructType dataType = (StructType) StructType.fromJson(rowSchema_json);
        StructField[] fields = dataType.fields();
        for(int i=0; i < fields.length; i++){
          StructField field = fields[i];
          Metadata meta = field.metadata();
          if (meta.contains("keyPath") && meta.contains("keyId")) {
            String keyPath = meta.getString("keyPath");
            String keyId = meta.getString("keyId");
            System.out.println(
                "Binhnt.loadCryptoConfiguration: field:  " + field.name() + ", keyId: " + keyId + ", keyPath = " + keyPath);
            Map<String, String> vault_data = vault.logical().read(keyPath).getData();
            String column_key = vault_data.get(keyId);
            this.secrets.put(field.name(),column_key);
            this.keyIds.put(field.name(),keyPath+"|"+keyId);
          }
        }
      }
    } catch (VaultException e) {
      System.out.println("Cannot create config ." + e.toString());
      return false;
    }
    return true;
  }

  @Override
  public FileEncryptionProperties getFileEncryptionProperties(
      Configuration fileHadoopConfig, Path tempFilePath, WriteSupport.WriteContext fileWriteContext)
      throws ParquetCryptoRuntimeException {
    Map<ColumnPath, ColumnEncryptionProperties> columnEncPropertiesMap = new HashMap<>();

    System.out.println(
        "Binhnt.getFileEncryptionProperties: tempFilePath = " + tempFilePath.toString());
    Map<String, String> extraMetaData = fileWriteContext.getExtraMetaData();

    loadCryptoConfiguration(fileHadoopConfig,extraMetaData);

    for (String columnName : this.secrets.keySet()) {
      String columnKey = (String) this.secrets.get(columnName);
      String keyId = this.keyIds.get(columnName);
      System.out.println(
          "binhnt.TekoPropertiesFactory: getFileEncryptionProperties columnName = "
              + columnName
              + ", columnKey = "
              + columnKey);
      if (!columnName.equals("footer_key")) {
        ColumnPath COL = ColumnPath.fromDotString(columnName);
        byte[] COL_KEY = columnKey.getBytes();
        ColumnEncryptionProperties COL_ENCR_PROPERTIES =
            ColumnEncryptionProperties.builder(COL.toDotString())
                .withKey(COL_KEY)
                .withKeyID(keyId)
                .build();
        columnEncPropertiesMap.put(COL, COL_ENCR_PROPERTIES);
      }
    }

    // System.out.println(
    //     "binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey = " + footerKey);
    // byte[] FOOTER_KEY = footerKey.getBytes();
    // FileEncryptionProperties.Builder fileEncBuilder =
    //     FileEncryptionProperties.builder(FOOTER_KEY);
    // return fileEncBuilder
    //     .withAlgorithm(ParquetCipher.AES_GCM_V1)
    //     .withEncryptedColumns(columnEncPropertiesMap)
    //     .withFooterKeyID("footer_key")
    //     .build();

    System.out.println(
        "binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey is null => withPlaintextFooter");
    FileEncryptionProperties.Builder fileEncBuilder =
        FileEncryptionProperties.builder("AAECAwQFBgcICQoLDA0ODw==".getBytes());
    return fileEncBuilder
        .withAlgorithm(ParquetCipher.AES_GCM_V1)
        .withEncryptedColumns(columnEncPropertiesMap)
        .withPlaintextFooter()
        .build();
  }

  @Override
  public FileDecryptionProperties getFileDecryptionProperties(
      Configuration hadoopConfig, Path filePath) throws ParquetCryptoRuntimeException {

    if (access_token == null) {
      loadConfiguration(hadoopConfig);
    }
    System.out.println("Binhnt.getFileDecryptionProperties: filePath = " + filePath.toString());

    // Initialize retriever
    VaultKeyIdRetriever kr1 = new VaultKeyIdRetriever(this.vault_url, this.access_token);
    FileDecryptionProperties.Builder fileDecBuilder = FileDecryptionProperties.builder();
    return fileDecBuilder.withKeyRetriever(kr1).withoutFooterSignatureVerification().build();
  }
}
