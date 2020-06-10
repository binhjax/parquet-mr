package com.teko.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.HashMap;
import java.util.Map;
import java.util.Hashtable;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Base64;

import org.apache.parquet.hadoop.api.WriteSupport;

import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.ColumnDecryptionProperties;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.FileDecryptionProperties;

import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ColumnEncryptionProperties;

import org.apache.parquet.crypto.DecryptionKeyRetriever;

import com.bettercloud.vault.SslConfig;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;

import com.bettercloud.vault.response.LogicalResponse;
import com.bettercloud.vault.response.AuthResponse;

public class TekoVaultFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {
    public static String KMS_INSTANCE_URL = "encryption.kms.instance.url";
    public static String KMS_USERNAME = "encryption.user.username";
    public static String KMS_PASSWORD = "encryption.user.password";
    public static String KMS_SECRET_PATH = "encryption.secrets.path";
    public static String CRYPTO_COLUMNS = "encryption.columns";


    String vault_url = null;

    String username = null;
    String password = null;

    String columns = null;
    String secrets_path = null;

    String access_token  = null;

    //Simple key retriever, based on UTF8 strings as key identifiers
    static class StringKeyIdRetriever implements DecryptionKeyRetriever{

       private final Hashtable<String,byte[]> keyMap = new Hashtable<String,byte[]>();

       public void putKey(String keyId, byte[] keyBytes) {
         keyMap.put(keyId, keyBytes);
       }

       @Override
       public byte[] getKey(byte[] keyMetaData) {
         String keyId = new String(keyMetaData, StandardCharsets.UTF_8);
         return keyMap.get(keyId);
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

          columns = conf.get(CRYPTO_COLUMNS);
          secrets_path = conf.get(KMS_SECRET_PATH);

          try {
              System.out.println("Binhnt.loadConfiguration: 1. Try to get access token. ");
              VaultConfig config =  new VaultConfig().address(vault_url)
                                        .sslConfig(new SslConfig().verify(false).build())
                                        .build();

              Vault vault = new Vault(config,2);

              AuthResponse response = vault.auth().loginByUserPass(username, password);

              access_token = response.getAuthClientToken();
              System.out.println("Binhnt.loadEncryptConfiguration:  2. Logged in with username =  " + username + ", token: " + access_token);

          } catch (VaultException e) {
            System.out.println("Binhnt.loadConfiguration: Cannot create config ." + e.toString());
            return;
          }
    }
    public  Map<String,String> loadEncryptConfiguration(Configuration conf) {
        if (access_token == null ){
            loadConfiguration(conf);
        }

        //Initialize keys
        String[] list = columns.replaceAll("\\s+","").split(",");

        Map<String, Object> secrets = new HashMap<String, Object>();

        for(int i=0; i < list.length; i++){
            String columnName = list[i].replaceAll("\\s+","");
            if (columnName != null ) {
                String columnKey =  generateKey();
                System.out.println("Binhnt.loadEncryptConfiguration:  Add column:  " + columnName + ", key = " + columnKey);
                secrets.put(columnName,columnKey);
           }
         }

        System.out.println("Binhnt.loadEncryptConfiguration:  2. Write data to vault:  " + secrets_path );
        try {
            VaultConfig config =  new VaultConfig()
                                      .address(vault_url)
                                      .token(access_token)
                                      .sslConfig(new SslConfig().verify(false).build())
                                      .build();

            Vault vault = new Vault(config,2);
            LogicalResponse writeResponse = vault.logical()
                                                  .write(secrets_path, secrets);
            Map<String, String> ret = writeResponse.getData();
            for (Map.Entry<String,String> entry : ret.entrySet())  {
                 System.out.println("Binhnt.loadEncryptConfiguration: ret.Key = " + entry.getKey() +  ", ret.Value = " + entry.getValue());
            }
            return loadDecryptConfiguration(conf);

        } catch (VaultException e) {
            System.out.println("Binhnt.loadEncryptConfiguration: Cannot create config ." + e.toString());
            return new HashMap<String, String>();
        }
    }

    public  Map<String,String> loadDecryptConfiguration(Configuration conf) {
        if (access_token == null ){
            loadConfiguration(conf);
        }
        try {
            System.out.println("Binhnt.loadDecryptConfiguration: 3. Read data from vault: " + secrets_path);
            VaultConfig config =  new VaultConfig()
                                      .address(vault_url)
                                      .token(access_token)
                                      .sslConfig(new SslConfig().verify(false).build())
                                      .build();
            Vault vault = new Vault(config,2);
            Map<String, String>  data = vault.logical()
                              .read(secrets_path)
                              .getData();

            for (Map.Entry<String,String> entry : data.entrySet())  {
                 System.out.println("Binhnt: Key = " + entry.getKey() +  ", Value = " + entry.getValue());
            }
            return data;
        } catch (VaultException e) {
             System.out.println("Cannot create config ." + e.toString());
             return new HashMap<String, String>();
        }
    }
    @Override
    public FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath,
                                                                WriteSupport.WriteContext fileWriteContext) throws ParquetCryptoRuntimeException {
      Map<ColumnPath, ColumnEncryptionProperties> columnEncPropertiesMap = new HashMap<>();

      System.out.println("Binhnt.getFileEncryptionProperties: tempFilePath = "+ tempFilePath.toString());

      Map<String, String>  config = loadEncryptConfiguration(fileHadoopConfig);

      for (String columnName : config.keySet()) {
           String columnKey = config.get(columnName);
           System.out.println("binhnt.TekoPropertiesFactory: getFileEncryptionProperties columnName = " + columnName + ", columnKey = " + columnKey);
           if (!columnName.equals("footer_key")) {
             ColumnPath COL = ColumnPath.fromDotString(columnName);
             byte[] COL_KEY = columnKey.getBytes();
             ColumnEncryptionProperties COL_ENCR_PROPERTIES = ColumnEncryptionProperties.builder(COL.toDotString()).withKey(COL_KEY).withKeyID(columnName).build();
             columnEncPropertiesMap.put(COL, COL_ENCR_PROPERTIES);
           }

      }
      System.out.println("binhnt.TekoPropertiesFactory: after loadConfiguration");

      String footerKey = config.get("footer_key");
      if (footerKey != null ) {
          System.out.println("binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey = " + footerKey);
          byte[] FOOTER_KEY = footerKey.getBytes();
          FileEncryptionProperties.Builder fileEncBuilder = FileEncryptionProperties.builder(FOOTER_KEY);
          return fileEncBuilder.withAlgorithm(ParquetCipher.AES_GCM_V1).withEncryptedColumns(columnEncPropertiesMap).withFooterKeyID("footer_key").build();
      }
      else {
          System.out.println("binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey is null => withPlaintextFooter");
          FileEncryptionProperties.Builder fileEncBuilder = FileEncryptionProperties.builder("AAECAwQFBgcICQoLDA0ODw==".getBytes());
          return fileEncBuilder.withAlgorithm(ParquetCipher.AES_GCM_V1).withEncryptedColumns(columnEncPropertiesMap).withPlaintextFooter().build();
      }
    }

    @Override
    public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)
      throws ParquetCryptoRuntimeException {

      System.out.println("Binhnt.getFileDecryptionProperties: filePath = "+ filePath.toString());
      //Load decryption from configuration
      Map<String, String>  config = loadDecryptConfiguration(hadoopConfig);

      //Initialize retriever
      StringKeyIdRetriever kr1 = new StringKeyIdRetriever();
      for (String columnName : config.keySet()) {
           System.out.println("binhnt.TekoPropertiesFactory: getFileDecryptionProperties:   columnName = " + columnName);
           String columnKey = config.get(columnName);
           kr1.putKey(columnName,columnKey.getBytes());
      }
      FileDecryptionProperties.Builder fileDecBuilder = FileDecryptionProperties.builder();

      String footerKey = config.get("footer_key");
      if (footerKey != null ) {
         return fileDecBuilder.withKeyRetriever(kr1).build();
      } else {
         return fileDecBuilder.withKeyRetriever(kr1).withoutFooterSignatureVerification().build();
      }
    }
}
