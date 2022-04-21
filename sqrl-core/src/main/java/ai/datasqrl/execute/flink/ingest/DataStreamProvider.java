package ai.datasqrl.execute.flink.ingest;

import ai.datasqrl.execute.flink.environment.FlinkStreamEngine;
import ai.datasqrl.io.formats.Format;
import ai.datasqrl.io.formats.TextLineFormat;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.io.impl.file.FilePath;
import ai.datasqrl.io.impl.kafka.KafkaSourceImplementation;
import ai.datasqrl.io.sources.DataSource;
import ai.datasqrl.io.sources.DataSourceConfiguration;
import ai.datasqrl.io.sources.DataSourceImplementation;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.SourceTableConfiguration;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.util.TimeAnnotatedRecord;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;


@AllArgsConstructor
public class DataStreamProvider {

    /*
    TODO: Rework to use Flink's FileSource and FileEnumerator based on SourceTableConfig
     */

  public DataStream<SourceRecord.Raw> getDataStream(SourceTable table,
      FlinkStreamEngine.Builder streamBuilder) {
    DataSource source = table.getDataset().getSource();
    DataSourceImplementation sourceImpl = source.getImplementation();
    DataSourceConfiguration sourceConfig = source.getConfig();
    SourceTableConfiguration tblConfig = table.getConfiguration();
    String flinkSourceName = String.join("-", table.getDataset().getName().getDisplay(),
        tblConfig.getIdentifier(), "input");
    Format.Parser parser = tblConfig.getFormatParser();
    StreamExecutionEnvironment env = streamBuilder.getEnvironment();
    if (sourceImpl instanceof DirectorySourceImplementation) {
      DirectorySourceImplementation filesource = (DirectorySourceImplementation) sourceImpl;

      //TODO: distinguish between text and byte input formats once we have AVRO,etc support
      DataStream<Format.Parser.Result> parsedStream;

      Duration monitorDuration = null;
//            if (filesource.getConfiguration().isDiscoverFiles()) monitorDuration = Duration.ofSeconds(10);
      FileEnumeratorProvider fileEnumerator = new FileEnumeratorProvider(filesource,
          source.getCanonicalizer(), tblConfig);

      if (parser instanceof TextLineFormat.Parser) {
        org.apache.flink.connector.file.src.FileSource.FileSourceBuilder<String> builder =
            org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(
                new org.apache.flink.connector.file.src.reader.TextLineFormat(
                    sourceConfig.getCharset()), FilePath.toFlinkPath(filesource.getPath()));

        builder.setFileEnumerator(fileEnumerator);
        if (monitorDuration != null) {
          builder.monitorContinuously(Duration.ofSeconds(10));
        }

        //TODO: set watermarks
//              stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SourceRecord>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getSourceTime().toEpochSecond()));

        DataStreamSource<String> textSource = env.fromSource(builder.build(),
            WatermarkStrategy.noWatermarks(), flinkSourceName);
        parsedStream = parseStream(textSource, (TextLineFormat.Parser) parser);
      } else {
        throw new UnsupportedOperationException("Unrecognized format: " + parser);
      }
      return parseToRecord(parsedStream);
    } else if (sourceImpl instanceof KafkaSourceImplementation) {
      KafkaSourceImplementation kafkaSource = (KafkaSourceImplementation) sourceImpl;
      DataStream<Format.Parser.Result> parsedStream;
      String topic = kafkaSource.getTopicPrefix() + tblConfig.getIdentifier();
      String groupId = flinkSourceName + "-" + streamBuilder.getUuid();

      if (parser instanceof TextLineFormat.Parser) {
        KafkaSourceBuilder<TimeAnnotatedRecord<String>> builder = KafkaSource.<TimeAnnotatedRecord<String>>builder()
            .setBootstrapServers(kafkaSource.getServersAsString())
            .setTopics(topic)
            .setStartingOffsets(OffsetsInitializer.earliest()) //TODO: work with commits
            .setGroupId(groupId);

        builder.setDeserializer(
            new KafkaTimeValueDeserializationSchemaWrapper<>(new SimpleStringSchema()));

        DataStreamSource<TimeAnnotatedRecord<String>> textSource = env.fromSource(builder.build(),
            WatermarkStrategy.noWatermarks(), flinkSourceName);
        parsedStream = parseTimeStream(textSource, (TextLineFormat.Parser) parser);
      } else {
        throw new UnsupportedOperationException("Unrecognized format: " + parser);
      }
      return parseToRecord(parsedStream);
    } else {
      throw new UnsupportedOperationException("Unrecognized source table type: " + table);
    }
  }

  public static DataStream<Format.Parser.Result> parseStream(DataStreamSource<String> textSource,
      TextLineFormat.Parser textparser) {
    return textSource.map(s -> textparser.parse(s)).filter(Format.Parser.Result::isSuccess);
  }

  public static DataStream<Format.Parser.Result> parseTimeStream(
      DataStreamSource<TimeAnnotatedRecord<String>> textSource, TextLineFormat.Parser textparser) {
    return textSource.map(t -> {
      Format.Parser.Result r = textparser.parse(t.getRecord());
      if (r.isSuccess() && !r.hasTime() && t.hasTime()) {
        return Format.Parser.Result.success(r.getRecord(), t.getSourceTime());
      } else {
        return r;
      }
    }).filter(r -> r.getType() == Format.Parser.Result.Type.SUCCESS);
  }

  public static DataStream<SourceRecord.Raw> parseToRecord(
      DataStream<Format.Parser.Result> parsedStream) {
    return parsedStream.map(r -> new SourceRecord.Raw(r.getRecord(), r.getSourceTime()));
  }

  @NoArgsConstructor
  @AllArgsConstructor
  private static class FileEnumeratorProvider implements
      org.apache.flink.connector.file.src.enumerate.FileEnumerator.Provider {

    DirectorySourceImplementation directorySource;
    NameCanonicalizer canonicalizer;
    SourceTableConfiguration table;

    @Override
    public org.apache.flink.connector.file.src.enumerate.FileEnumerator create() {
      return new NonSplittingRecursiveEnumerator(new FileNameMatcher());
    }

    private class FileNameMatcher implements Predicate<Path> {

      @Override
      public boolean test(Path path) {
        try {
          if (path.getFileSystem().getFileStatus(path).isDir()) {
            return true;
          }
        } catch (IOException e) {
          return false;
        }
        return directorySource.isTableFile(FilePath.fromFlinkPath(path), table, canonicalizer);
      }
    }
  }


  static class KafkaTimeValueDeserializationSchemaWrapper<T> implements
      KafkaRecordDeserializationSchema<TimeAnnotatedRecord<T>> {

    private static final long serialVersionUID = 1L;
    private final DeserializationSchema<T> deserializationSchema;

    KafkaTimeValueDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
      this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
      deserializationSchema.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message,
        Collector<TimeAnnotatedRecord<T>> out)
        throws IOException {
      T result = deserializationSchema.deserialize(message.value());
      if (result != null) {
        out.collect(new TimeAnnotatedRecord<>(result, Instant.ofEpochSecond(message.timestamp())));
      }
    }

    @Override
    public TypeInformation<TimeAnnotatedRecord<T>> getProducedType() {
      return (TypeInformation) TypeInformation.of(TimeAnnotatedRecord.class);
      //return deserializationSchema.getProducedType();
    }
  }

}
