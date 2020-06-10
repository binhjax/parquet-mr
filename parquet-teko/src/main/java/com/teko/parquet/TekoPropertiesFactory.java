package com.teko.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.HashMap;
import java.util.Map;

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


public class TekoPropertiesFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {
    public static String CRYPTO_KEY_LIST = "encryption.key.list";
    public static String CRYPTO_COLUMN_KEYS = "encryption.column.keys";
    public static String CRYPTO_FOOTER_KEY = "encryption.footer.key";
    private static byte[] DEFAULT_FOOTER_KEY = {0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
      0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10};

    Map<String, String> keyList = new HashMap<String, String>();
    Map<String, String> columnKeys = new HashMap<String, String>();
    String footerKey = null;

    public Map<String, String> parseToMap(String data) {
      Map<String, String>  map = new HashMap<String, String>();
      String[] list = data.replaceAll("\\s+","").split(",");
      for(int i=0; i < list.length; i++){
          String item = list[i].replaceAll("\\s+","");
          if (item != null ) {
              String[] key_value =  item.split(":");
              if (key_value.length == 2){
                  map.put(key_value[0].replaceAll("\\s+",""),key_value[1].replaceAll("\\s+",""));
              }
         }
       }
       return map;
    }
    public void loadKeyList(Configuration conf) {
        String list_str = conf.get(CRYPTO_KEY_LIST);
        if (list_str != null) {
            System.out.println("binhnt.TekoPropertiesFactory: loadKeyList: list_str = " + list_str);
            keyList = parseToMap(list_str);
        }
    }
    public void loadColumnKeys(Configuration conf) {
        String column_key_str = conf.get(CRYPTO_COLUMN_KEYS);
        if (column_key_str != null) {
            System.out.println("binhnt.TekoPropertiesFactory: loadKeyList: column_key_str = " + column_key_str);
            columnKeys = parseToMap(column_key_str);
        }
    }
    public void loadFooterKey(Configuration conf) {
      footerKey = conf.get(CRYPTO_FOOTER_KEY);
    }
    public void loadConfiguration(Configuration conf) {
        loadKeyList(conf);
        loadColumnKeys(conf);
        loadFooterKey(conf);
    }

    @Override
    public FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath,
                                                                WriteSupport.WriteContext fileWriteContext) throws ParquetCryptoRuntimeException {
      loadConfiguration(fileHadoopConfig);
      Map<ColumnPath, ColumnEncryptionProperties> columnEncPropertiesMap = new HashMap<>();

      for (String keyId : columnKeys.keySet()) {
           String columnName = columnKeys.get(keyId);
           String columnKey = keyList.get(keyId);
           System.out.println("binhnt.TekoPropertiesFactory: getFileEncryptionProperties columnName = " + columnName + ", columnKey = " + columnKey);

           ColumnPath COL = ColumnPath.fromDotString(columnName);

           byte[] COL_KEY = columnKey.getBytes();
           ColumnEncryptionProperties COL_ENCR_PROPERTIES = ColumnEncryptionProperties.builder(COL.toDotString()).withKey(COL_KEY).withKeyID(keyId).build();
           columnEncPropertiesMap.put(COL, COL_ENCR_PROPERTIES);
      }
      System.out.println("binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey = " + footerKey);

      String footerKey_str = keyList.get(footerKey);
      byte[] FOOTER_KEY = null;

      if (footerKey_str != null ) {
          System.out.println("binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey_str = " + footerKey_str);
          FOOTER_KEY = footerKey_str.getBytes();
          FileEncryptionProperties.Builder fileEncBuilder = FileEncryptionProperties.builder(FOOTER_KEY);
          return fileEncBuilder.withAlgorithm(ParquetCipher.AES_GCM_V1).withEncryptedColumns(columnEncPropertiesMap).build();
      }
      else {
          System.out.println("binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey_str is null => withPlaintextFooter");
          FileEncryptionProperties.Builder fileEncBuilder = FileEncryptionProperties.builder(DEFAULT_FOOTER_KEY);
          return fileEncBuilder.withAlgorithm(ParquetCipher.AES_GCM_V1).withEncryptedColumns(columnEncPropertiesMap).withPlaintextFooter().build();
      }
    }

    @Override
    public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)
      throws ParquetCryptoRuntimeException {

      loadConfiguration(hadoopConfig);

      System.out.println("binhnt.TekoPropertiesFactory: after loadConfiguration:   footerKey = " + footerKey);
      Map<ColumnPath, ColumnDecryptionProperties> columnDecPropertiesMap = new HashMap<>();

      for (String key_id : columnKeys.keySet()) {
           System.out.println("binhnt.TekoPropertiesFactory: getFileDecryptionProperties  ");
           String columnName = columnKeys.get(key_id);
           String columnKey = keyList.get(key_id);
           System.out.println("binhnt.TekoPropertiesFactory: columnName = " + columnName + ", columnKey = " + columnKey);

           ColumnPath COL = ColumnPath.fromDotString(columnName);

           byte[] COL_KEY = columnKey.getBytes();
           ColumnDecryptionProperties COL_DECR_PROPERTIES = ColumnDecryptionProperties.builder(COL.toDotString()).withKey(COL_KEY).build();
           columnDecPropertiesMap.put(COL, COL_DECR_PROPERTIES);
      }

      FileDecryptionProperties.Builder fileDecBuilder = FileDecryptionProperties.builder();

      String footerKey_str = keyList.get(footerKey);
      if (footerKey_str != null ) {
            byte[] FOOTER_KEY = footerKey_str.getBytes();
          return fileDecBuilder.withFooterKey(FOOTER_KEY).withColumnKeys(columnDecPropertiesMap).build();
      } else {
          return fileDecBuilder.withoutFooterSignatureVerification().withColumnKeys(columnDecPropertiesMap).build();
      }
    }
}
