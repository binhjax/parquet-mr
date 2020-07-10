package org.apache.parquet.crypto;

import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EncryptionPropertiesFactory interface enables transparent activation of Parquet encryption.
 *
 * <p>Its customized implementations produce encryption properties for each Parquet file, using the
 * input information available in Parquet file writers: file path, file extended schema - and also
 * Hadoop configuration properties that can pass custom parameters required by a crypto factory. A
 * factory implementation can use or ignore any of these inputs.
 *
 * <p>The example usage could be as below. 1. Write a class to implement
 * EncryptionPropertiesFactory. 2. Set configuration of "parquet.crypto.factory.class" with the
 * fully qualified name of this class. For example, we can set the configuration in SparkSession as
 * below. SparkSession spark = SparkSession .config("parquet.crypto.factory.class",
 * "xxx.xxx.EncryptionPropertiesClassLoaderImpl")
 *
 * <p>The implementation of this interface will be instantiated by {@link
 * #loadFactory(Configuration)}.
 */
public interface EncryptionPropertiesFactory {

  Logger LOG = LoggerFactory.getLogger(EncryptionPropertiesFactory.class);
  String CRYPTO_FACTORY_CLASS_PROPERTY_NAME = "parquet.crypto.factory.class";

  /**
   * Load EncryptionPropertiesFactory class specified by CRYPTO_FACTORY_CLASS_PROPERTY_NAME as the
   * path in the configuration
   *
   * @param conf Configuration where user specifies the class path
   * @return object with class EncryptionPropertiesFactory if user specified the class path and
   *     invoking of the class succeeds. Null if user doesn't specify the class path (no encryption
   *     then).
   * @throws BadConfigurationException if the instantiation of the configured class fails
   */
  static EncryptionPropertiesFactory loadFactory(Configuration conf) {
    final Class<?> encryptionPropertiesFactoryClass =
        ConfigurationUtil.getClassFromConfig(
            conf, CRYPTO_FACTORY_CLASS_PROPERTY_NAME, EncryptionPropertiesFactory.class);

    System.out.println(
        "Binhnt.EncryptionPropertiesFactory.loadFactory: Try to load encryptionPropertiesFactoryClass");
    if (null == encryptionPropertiesFactoryClass) {
      System.out.println(
          "Binhnt.EncryptionPropertiesFactory.loadFactory: EncryptionPropertiesFactory is not configured - name not found in hadoop config");
      LOG.debug("EncryptionPropertiesFactory is not configured - name not found in hadoop config");
      return null;
    }

    try {
      System.out.println("Binhnt.EncryptionPropertiesFactory.loadFactory: Create instance");
      return (EncryptionPropertiesFactory) encryptionPropertiesFactoryClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      System.out.println("Binhnt.EncryptionPropertiesFactory.loadFactory: Exception");

      throw new BadConfigurationException(
          "could not instantiate encryptionPropertiesFactoryClass class: "
              + encryptionPropertiesFactoryClass,
          e);
    }
  }

  /**
   * Get FileEncryptionProperties object which is created by the implementation of this interface.
   * Please see the unit test SampleEncryptionPropertiesFactory for example
   *
   * @param fileHadoopConfig Configuration that is used to pass the needed information, e.g. KMS uri
   * @param tempFilePath File path of the parquet file being written. Can be used for AAD prefix
   *     creation, key material management, etc. Implementations must not presume the path is
   *     permanent, as the file can be moved or renamed later
   * @param fileWriteContext WriteContext to provide information like schema to build the
   *     FileEncryptionProperties
   * @return object with class of FileEncryptionProperties. Null return value means the file should
   *     not be encrypted.
   * @throws ParquetCryptoRuntimeException if there is an exception while creating the object
   */
  FileEncryptionProperties getFileEncryptionProperties(
      Configuration fileHadoopConfig, Path tempFilePath, WriteContext fileWriteContext)
      throws ParquetCryptoRuntimeException;
}
