package org.apache.parquet.hadoop.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.schema.MessageType;

/**
 *
 * Context passed to ReadSupport when initializing for read
 */
public class InitContext {

  private final Map<String,Set<String>> keyValueMetadata;
  private Map<String,String> mergedKeyValueMetadata;
  private final Configuration configuration;
  private final MessageType fileSchema;

  /**
   * @param configuration the hadoop configuration
   * @param keyValueMetadata extra metadata from file footers
   * @param fileSchema the merged schema from the files
   */
  public InitContext(
      Configuration configuration,
      Map<String, Set<String>> keyValueMetadata,
      MessageType fileSchema) {
    super();
    this.keyValueMetadata = keyValueMetadata;
    this.configuration = configuration;
    this.fileSchema = fileSchema;
  }

  /**
   * If there is a conflicting value when reading from multiple files,
   * an exception will be thrown
   * @return the merged key values metadata form the file footers
   */
  @Deprecated
  public Map<String, String> getMergedKeyValueMetaData() {
    if (mergedKeyValueMetadata == null) {
      Map<String, String> mergedKeyValues = new HashMap<String, String>();
      for (Entry<String, Set<String>> entry : keyValueMetadata.entrySet()) {
        if (entry.getValue().size() > 1) {
          throw new RuntimeException("could not merge metadata: key " + entry.getKey() + " has conflicting values: " + entry.getValue());
        }
        mergedKeyValues.put(entry.getKey(), entry.getValue().iterator().next());
      }
      mergedKeyValueMetadata = mergedKeyValues;
    }
    return mergedKeyValueMetadata;
  }

  /**
   * @return the configuration for this job
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * this is the union of all the schemas when reading multiple files.
   * @return the schema of the files being read
   */
  public MessageType getFileSchema() {
    return fileSchema;
  }

  /**
   * each key is associated with the list of distinct values found in footers
   * @return the merged metadata from the footer of the file
   */
  public Map<String, Set<String>> getKeyValueMetadata() {
    return keyValueMetadata;
  }

}
