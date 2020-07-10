package org.apache.parquet.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

/**
 * Writes records to a Parquet file
 *
 * @see ParquetOutputFormat
 *
 * @param <T> the type of the materialized records
 */
public class ParquetRecordWriter<T> extends RecordWriter<Void, T> {

  private final InternalParquetRecordWriter<T> internalWriter;
  private final MemoryManager memoryManager;
  private final CodecFactory codecFactory;

  /**
   *
   * @param w the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param blockSize the size of a block in the file (this will be approximate)
   * @param pageSize the size of a page in the file (this will be approximate)
   * @param compressor the compressor used to compress the pages
   * @param dictionaryPageSize the threshold for dictionary size
   * @param enableDictionary to enable the dictionary
   * @param validating if schema validation should be turned on
   * @param writerVersion writer compatibility version
   */
  @Deprecated
  public ParquetRecordWriter(
      ParquetFileWriter w,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetaData,
      int blockSize, int pageSize,
      BytesCompressor compressor,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion) {
    System.out.println("ParquetRecordWriter.construct: start 1");
    ParquetProperties props = ParquetProperties.builder()
        .withPageSize(pageSize)
        .withDictionaryPageSize(dictionaryPageSize)
        .withDictionaryEncoding(enableDictionary)
        .withWriterVersion(writerVersion)
        .build();
    internalWriter = new InternalParquetRecordWriter<T>(w, writeSupport, schema,
        extraMetaData, blockSize, compressor, validating, props);
    this.memoryManager = null;
    this.codecFactory = null;
  }

  /**
   *
   * @param w the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param blockSize the size of a block in the file (this will be approximate)
   * @param pageSize the size of a page in the file (this will be approximate)
   * @param compressor the compressor used to compress the pages
   * @param dictionaryPageSize the threshold for dictionary size
   * @param enableDictionary to enable the dictionary
   * @param validating if schema validation should be turned on
   * @param writerVersion writer compatibility version
   * @param memoryManager memory manager for the write
   */
  @Deprecated
  public ParquetRecordWriter(
      ParquetFileWriter w,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetaData,
      long blockSize, int pageSize,
      BytesCompressor compressor,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion,
      MemoryManager memoryManager) {

    System.out.println("ParquetRecordWriter.construct: start 2");
    ParquetProperties props = ParquetProperties.builder()
        .withPageSize(pageSize)
        .withDictionaryPageSize(dictionaryPageSize)
        .withDictionaryEncoding(enableDictionary)
        .withWriterVersion(writerVersion)
        .build();
    internalWriter = new InternalParquetRecordWriter<T>(w, writeSupport, schema,
        extraMetaData, blockSize, compressor, validating, props);
    this.memoryManager = Objects.requireNonNull(memoryManager, "memoryManager cannot be null");
    memoryManager.addWriter(internalWriter, blockSize);
    this.codecFactory = null;
  }

  /**
   *
   * @param w the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param blockSize the size of a block in the file (this will be approximate)
   * @param codec the compression codec used to compress the pages
   * @param validating if schema validation should be turned on
   * @param props parquet encoding properties
   */
  ParquetRecordWriter(
      ParquetFileWriter w,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetaData,
      long blockSize,
      CompressionCodecName codec,
      boolean validating,
      ParquetProperties props,
      MemoryManager memoryManager,
      Configuration conf) {

    System.out.println("ParquetRecordWriter.construct: start 3");
    this.codecFactory = new CodecFactory(conf, props.getPageSizeThreshold());
    internalWriter = new InternalParquetRecordWriter<T>(w, writeSupport, schema,
        extraMetaData, blockSize, codecFactory.getCompressor(codec), validating,
        props);
    this.memoryManager = Objects.requireNonNull(memoryManager, "memoryManager cannot be null");
    memoryManager.addWriter(internalWriter, blockSize);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      internalWriter.close();
      // release after the writer closes in case it is used for a last flush
    } finally {
      if (codecFactory != null) {
        codecFactory.release();
      }
      if (memoryManager != null) {
        memoryManager.removeWriter(internalWriter);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(Void key, T value) throws IOException, InterruptedException {
    System.out.println("ParquetRecordWriter.write: call internalWriter.write");
    internalWriter.write(value);
  }

}
