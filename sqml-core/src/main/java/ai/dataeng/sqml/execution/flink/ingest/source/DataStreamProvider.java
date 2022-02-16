package ai.dataeng.sqml.execution.flink.ingest.source;

import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.io.sources.impl.file.FileTableConfiguration;
import ai.dataeng.sqml.io.sources.impl.file.FileType;
import com.google.common.collect.Iterators;
import lombok.AllArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
public class DataStreamProvider {


    public DataStream<SourceRecord.Raw> getDataStream(SourceTable table, StreamExecutionEnvironment env) {
        if (table.getConfiguration() instanceof FileTableConfiguration) {
            FileTableConfiguration fileTable = (FileTableConfiguration)table.getConfiguration();
            FileSourceConfiguration source = (FileSourceConfiguration)table.getDataset().getConfiguration();
            Iterator<SourceRecord.Raw> input;
            try {
                input = Iterators.concat(
                        Iterators.transform(source.getTableFiles(fileTable, 0).iterator()
                                , nf -> fileToRecordIterator(nf.getFile(), fileTable.getFileType())));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            DataStreamSource<SourceRecord.Raw> stream = env.fromCollection(input,SourceRecord.Raw.class);
//        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SourceRecord>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getSourceTime().toEpochSecond()));
            return stream;
        } else throw new UnsupportedOperationException("Unrecognized source table type: " + table);
    }

    private Iterator<SourceRecord.Raw> fileToRecordIterator(Path file, FileType fileType) {
        final Instant fileTime;
        try {
            fileTime = Files.getLastModifiedTime(file).toInstant();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<Map<String, Object>> data = fileType.getRecords(file);
        return Iterators.transform(data.listIterator(),r -> new SourceRecord.Raw(r, fileTime));
    }

}
