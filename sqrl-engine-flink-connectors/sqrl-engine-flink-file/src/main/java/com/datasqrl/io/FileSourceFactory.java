package com.datasqrl.io;

import com.datasqrl.config.SourceFactory;
import com.datasqrl.config.SourceServiceLoader.SourceFactoryContext;
import com.datasqrl.engine.stream.flink.FlinkSourceFactoryContext;
import com.datasqrl.engine.stream.inmemory.io.FileStreamUtil;
import com.datasqrl.io.formats.FileFormat;
import com.datasqrl.io.impl.file.DirectoryDataSystem.DirectoryConnector;
import com.datasqrl.io.impl.file.FilePath;
import com.datasqrl.io.impl.file.FilePathConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.function.Predicate;

public class FileSourceFactory implements
    SourceFactory<SingleOutputStreamOperator<TimeAnnotatedRecord<String>>> {

  @Override
  public String getEngine() {
    return "flink";
  }

  @Override
  public String getSourceName() {
    return "file";
  }

  @Override
  public SingleOutputStreamOperator<TimeAnnotatedRecord<String>> create(DataSystemConnector connector, SourceFactoryContext context) {
    DirectoryConnector filesource = (DirectoryConnector) connector;
    FlinkSourceFactoryContext ctx = (FlinkSourceFactoryContext) context;

    FilePathConfig pathConfig = filesource.getPathConfig();
    if (pathConfig.isURL()) {
      Preconditions.checkArgument(!pathConfig.isDirectory());
      return ctx.getEnv().fromCollection(pathConfig.getFiles(filesource, ctx.getTable().getConfiguration())).
          flatMap(new ReadPathByLine())
          .map(new NoTimedRecord());
    } else {
      org.apache.flink.connector.file.src.FileSource.FileSourceBuilder<String> builder;
      if (pathConfig.isDirectory()) {
        StreamFormat<String> format;
        if (ctx.getTable().getConfiguration().getFormat().getFileFormat() == FileFormat.JSON) {
          format = new JsonInputFormat(ctx.getTable().getConfiguration().getCharset());
        } else {
          format = new org.apache.flink.connector.file.src.reader.TextLineInputFormat(
              ctx.getTable().getConfiguration().getCharset());
        }

        builder = org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(
            format,
            FilePath.toFlinkPath(pathConfig.getDirectory()));
        Duration monitorDuration = null;
        FileEnumeratorProvider fileEnumerator = new FileEnumeratorProvider(filesource,
            ctx.getTable().getConfiguration());
        builder.setFileEnumerator(fileEnumerator);
        if (monitorDuration != null) {
          builder.monitorContinuously(Duration.ofSeconds(10));
        }
      } else {
        Path[] inputPaths = pathConfig.getFiles(filesource, ctx.getTable().getConfiguration()).stream()
            .map(FilePath::toFlinkPath).toArray(size -> new Path[size]);
        builder = org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(
            new org.apache.flink.connector.file.src.reader.TextLineInputFormat(
                ctx.getTable().getConfiguration().getCharset()), inputPaths);
      }

      return ctx.getEnv().fromSource(builder.build(),
          WatermarkStrategy.noWatermarks(), ctx.getFlinkName())
          .map(new NoTimedRecord())
//          .setParallelism(4)//todo config
          ;
    }
  }

  @NoArgsConstructor
  @AllArgsConstructor
  public static class FileEnumeratorProvider implements
      org.apache.flink.connector.file.src.enumerate.FileEnumerator.Provider {

    DirectoryConnector directorySource;
    TableConfig table;

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
        return directorySource.isTableFile(FilePath.fromFlinkPath(path), table);
      }
    }
  }

  public static class NoTimedRecord implements MapFunction<String, TimeAnnotatedRecord<String>> {

    @Override
    public TimeAnnotatedRecord<String> map(String s) throws Exception {
      return new TimeAnnotatedRecord<>(s, null);
    }
  }


  public class ReadPathByLine implements FlatMapFunction<FilePath, String> {
    @Override
    public void flatMap(FilePath filePath, Collector<String> collector) throws Exception {
      try (InputStream is = filePath.read()) {
        FileStreamUtil.readByLine(is).forEach(collector::collect);
      }
    }
  }
}
