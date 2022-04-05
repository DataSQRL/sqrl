package ai.dataeng.sqml.execution.flink.ingest;

import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.formats.Format;
import ai.dataeng.sqml.io.formats.TextLineFormat;
import ai.dataeng.sqml.io.impl.file.FilePath;
import ai.dataeng.sqml.io.impl.file.FileSource;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Predicate;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


@AllArgsConstructor
public class DataStreamProvider {

    /*
    TODO: Rework to use Flink's FileSource and FileEnumerator based on SourceTableConfig
     */

    public DataStream<SourceRecord.Raw> getDataStream(SourceTable table, StreamExecutionEnvironment env) {
        DataSource source = table.getDataset().getSource();
        SourceTableConfiguration tblConfig = table.getConfiguration();
        String flinkSourceName = String.join("-",table.getDataset().getName().getDisplay(),tblConfig.getIdentifier(),"input");
        if (source instanceof FileSource) {
            FileSource filesource = (FileSource)source;

            //TODO: distinguish between text and byte input formats once we have AVRO,etc support
            Format.Parser parser = tblConfig.getFormatParser();
            DataStream<Format.Parser.Result> parsedStream;

            Duration monitorDuration = null;
//            if (filesource.getConfiguration().isDiscoverFiles()) monitorDuration = Duration.ofSeconds(10);
            FileEnumeratorProvider fileEnumerator = new FileEnumeratorProvider(filesource,tblConfig);

            if (parser instanceof TextLineFormat.Parser) {
                TextLineFormat.Parser textparser = (TextLineFormat.Parser)parser;

                org.apache.flink.connector.file.src.FileSource.FileSourceBuilder<String> builder =
                        org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(
                                new org.apache.flink.connector.file.src.reader.TextLineFormat(), FilePath.toFlinkPath(filesource.getDirectoryPath()));

                builder.setFileEnumerator(fileEnumerator);
                if (monitorDuration!=null) builder.monitorContinuously(Duration.ofSeconds(10));

                //TODO: set watermarks
//              stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SourceRecord>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getSourceTime().toEpochSecond()));

                DataStreamSource<String> textSource = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), flinkSourceName);
                parsedStream = textSource.map(s -> textparser.parse(s)).filter(r -> r.getType() == Format.Parser.Result.Type.SUCCESS);
            } else throw new UnsupportedOperationException("Unrecognized format: " + parser);

            DataStream<SourceRecord.Raw> stream = parsedStream.map(r -> new SourceRecord.Raw(r.getRecord(), r.getSource_time()));
            return stream;
        } else throw new UnsupportedOperationException("Unrecognized source table type: " + table);
    }


    @NoArgsConstructor
    @AllArgsConstructor
    private static class FileEnumeratorProvider implements org.apache.flink.connector.file.src.enumerate.FileEnumerator.Provider {

        FileSource fileSource;
        SourceTableConfiguration table;

        @Override
        public org.apache.flink.connector.file.src.enumerate.FileEnumerator create() {
            return new NonSplittingRecursiveEnumerator(new FileNameMatcher());
        }

        private class FileNameMatcher implements Predicate<Path> {

            @Override
            public boolean test(Path path) {
                try {
                    if (path.getFileSystem().getFileStatus(path).isDir()) return true;
                } catch (IOException e) {
                    return false;
                }
                return fileSource.isTableFile(FilePath.fromFlinkPath(path),table);
            }
        }
    }


}
