package org.apache.parquet.hadoop.api;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * Abstraction used by the {@link org.apache.parquet.hadoop.ParquetInputFormat} to materialize records
 *
 * @param <T> the type of the materialized record
 */
abstract public class ReadSupport<T> {

  /**
   * configuration key for a parquet read projection schema
   */
	public static final String PARQUET_READ_SCHEMA = "parquet.read.schema";

  /**
   * attempts to validate and construct a {@link MessageType} from a read projection schema
   *
   * @param fileMessageType         the typed schema of the source
   * @param partialReadSchemaString the requested projection schema
   * @return the typed schema that should be used to read
   */
  public static MessageType getSchemaForRead(MessageType fileMessageType, String partialReadSchemaString) {
    if (partialReadSchemaString == null)
      return fileMessageType;
    MessageType requestedMessageType = MessageTypeParser.parseMessageType(partialReadSchemaString);
    return getSchemaForRead(fileMessageType, requestedMessageType);
  }

  public static MessageType getSchemaForRead(MessageType fileMessageType, MessageType projectedMessageType) {
    fileMessageType.checkContains(projectedMessageType);
    return projectedMessageType;
  }

  /**
   * called in {@link org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)} in the front end
   *
   * @param configuration    the job configuration
   * @param keyValueMetaData the app specific metadata from the file
   * @param fileSchema       the schema of the file
   * @return the readContext that defines how to read the file
   *
   * @deprecated override {@link ReadSupport#init(InitContext)} instead
   */
  @Deprecated
  public ReadContext init(
          Configuration configuration,
          Map<String, String> keyValueMetaData,
          MessageType fileSchema) {
    throw new UnsupportedOperationException("Override init(InitContext)");
  }

  /**
   * called in {@link org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)} in the front end
   *
   * @param context the initialisation context
   * @return the readContext that defines how to read the file
   */
  public ReadContext init(InitContext context) {
    return init(context.getConfiguration(), context.getMergedKeyValueMetaData(), context.getFileSchema());
  }

  /**
   * called in {@link org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)} in the back end
   * the returned RecordMaterializer will materialize the records and add them to the destination
   *
   * @param configuration    the job configuration
   * @param keyValueMetaData the app specific metadata from the file
   * @param fileSchema       the schema of the file
   * @param readContext      returned by the init method
   * @return the recordMaterializer that will materialize the records
   */
  abstract public RecordMaterializer<T> prepareForRead(
          Configuration configuration,
          Map<String, String> keyValueMetaData,
          MessageType fileSchema,
          ReadContext readContext);

  /**
   * information to read the file
   */
  public static final class ReadContext {
    private final MessageType requestedSchema;
    private final Map<String, String> readSupportMetadata;

    /**
     * @param requestedSchema the schema requested by the user. Can not be null.
     */
    public ReadContext(MessageType requestedSchema) {
      this(requestedSchema, null);
    }

    /**
     * @param requestedSchema the schema requested by the user. Can not be null.
     * @param readSupportMetadata metadata specific to the ReadSupport implementation. Will be available in the prepareForRead phase.
     */
    public ReadContext(MessageType requestedSchema, Map<String, String> readSupportMetadata) {
      super();
      if (requestedSchema == null) {
        throw new NullPointerException("requestedSchema");
      }
      this.requestedSchema = requestedSchema;
      this.readSupportMetadata = readSupportMetadata;
    }

    /**
     * @return the schema of the file
     */
    public MessageType getRequestedSchema() {
      return requestedSchema;
    }

    /**
     * @return metadata specific to the ReadSupport implementation
     */
    public Map<String, String> getReadSupportMetadata() {
      return readSupportMetadata;
    }
  }
}
