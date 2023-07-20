package com.datasqrl.io;

import com.datasqrl.config.DataStreamSourceFactory;
import com.datasqrl.config.FlinkSourceFactoryContext;
import com.datasqrl.config.SourceFactory;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.impl.file.FileDataSystemConfig;
import com.datasqrl.io.impl.file.FileDataSystemDiscovery;
import com.datasqrl.io.impl.file.FileDataSystemFactory;
import com.datasqrl.io.impl.file.FilePath;
import com.datasqrl.io.impl.file.FilePathConfig;
import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.util.FileStreamUtil;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TimeUtils;


@AutoService(SourceFactory.class)
public class FileSourceFactory implements DataStreamSourceFactory {

  @Override
  public String getSourceName() {
    return FileDataSystemFactory.SYSTEM_NAME;
  }

  @Override
  public SingleOutputStreamOperator<TimeAnnotatedRecord<String>> create(FlinkSourceFactoryContext ctx) {
    TableConfig tableConfig = ctx.getTableConfig();
    FilePathConfig pathConfig = FileDataSystemConfig.fromConfig(tableConfig).getFilePath(tableConfig.getErrors());
    FormatFactory formatFactory = tableConfig.getFormat();
    Preconditions.checkArgument(formatFactory instanceof TextLineFormat,"This connector only supports text files");
    String charset = ((TextLineFormat)formatFactory).getCharset(tableConfig.getFormatConfig()).name();

    if (pathConfig.isURL()) {
      Preconditions.checkArgument(!pathConfig.isDirectory());
      return ctx.getEnv().fromCollection(pathConfig.getFiles(ctx.getTableConfig())).
          flatMap(new ReadPathByLine(charset))
          .map(new NoTimedRecord());
    } else {
      org.apache.flink.connector.file.src.FileSource.FileSourceBuilder<String> builder;
      if (pathConfig.isDirectory()) {
        StreamFormat<String> format;
        if (formatFactory.getName().equalsIgnoreCase("json")) {
          format = new JsonInputFormat(charset);
        } else {
          format = new org.apache.flink.connector.file.src.reader.TextLineInputFormat(
              charset);
        }

        builder = org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(
            format,
            FilePath.toFlinkPath(pathConfig.getDirectory()));

        FileEnumeratorProvider fileEnumerator = new FileEnumeratorProvider(tableConfig.getBase(),
            tableConfig.getFormat(),
            FileDataSystemConfig.fromConfig(tableConfig));
        builder.setFileEnumerator(fileEnumerator);

        Optional<String> monitorInterval = tableConfig.getConnectorConfig()
            .asString(FileSystemConnectorOptions.SOURCE_MONITOR_INTERVAL.key())
            .getOptional();
        if (isMonitor(monitorInterval)) {
          Duration duration = monitorInterval
              .map(TimeUtils::parseDuration)
              .orElse(Duration.ofSeconds(10));
          builder.monitorContinuously(duration);
        }
      } else {
        Path[] inputPaths = pathConfig.getFiles(ctx.getTableConfig()).stream()
            .map(FilePath::toFlinkPath).toArray(size -> new Path[size]);
        builder = org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(
            new org.apache.flink.connector.file.src.reader.TextLineInputFormat(
                charset), inputPaths);
      }

      return ctx.getEnv().fromSource(builder.build(),
          WatermarkStrategy.noWatermarks(), ctx.getFlinkName())
          .map(new NoTimedRecord())
//          .setParallelism(4)//todo config
          ;
    }
  }

  /**
   * Monitor if configuration is missing or non zero
   */
  private boolean isMonitor(Optional<String> monitorInterval) {
    return monitorInterval
        .map(s -> !TimeUtils.parseDuration(s).equals(Duration.ZERO))
        .orElse(true);
  }

  @NoArgsConstructor
  @AllArgsConstructor
  public static class FileEnumeratorProvider implements
      org.apache.flink.connector.file.src.enumerate.FileEnumerator.Provider {

    BaseTableConfig baseConfig;
    FormatFactory format;
    FileDataSystemConfig fileConfig;

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
        return FileDataSystemDiscovery.isTableFile(FilePath.fromFlinkPath(path), baseConfig, format, fileConfig);
      }
    }
  }

  public static class NoTimedRecord implements MapFunction<String, TimeAnnotatedRecord<String>> {

    @Override
    public TimeAnnotatedRecord<String> map(String s) throws Exception {
      return new TimeAnnotatedRecord<>(s, null);
    }
  }


  @AllArgsConstructor
  public class ReadPathByLine implements FlatMapFunction<FilePath, String> {

    private String charset;

    @Override
    public void flatMap(FilePath filePath, Collector<String> collector) throws Exception {
      try (InputStream is = filePath.read()) {
        FileStreamUtil.readByLine(is, charset).forEach(collector::collect);
      }
    }
  }
}
