package ai.dataeng.sqml.execution.flink.ingest;

import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.impl.file.FileFormat;
import ai.dataeng.sqml.io.sources.impl.file.FileSource;
import ai.dataeng.sqml.io.sources.impl.file.FileTableConfiguration;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@AllArgsConstructor
public class DataStreamProvider {


    public DataStream<SourceRecord.Raw> getDataStream(SourceTable table, StreamExecutionEnvironment env) {
        if (table.getConfiguration() instanceof FileTableConfiguration) {
            FileTableConfiguration fileTable = (FileTableConfiguration)table.getConfiguration();
            FileSource source = (FileSource)table.getDataset().getConfiguration();
            Iterator<SourceRecord.Raw> input;
            try {
                input = Iterators.concat(
                        Iterators.transform(source.getTableFiles(fileTable, 0).iterator()
                                , nf -> fileToRecordIterator(nf.getFile(), fileTable.getFileFormat())));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            List<SourceRecord.Raw> allItems = Lists.newArrayList(input);
            DataStreamSource<SourceRecord.Raw> stream = env.fromCollection(allItems);
//        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SourceRecord>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getSourceTime().toEpochSecond()));
            return stream;
        } else throw new UnsupportedOperationException("Unrecognized source table type: " + table);
    }

    private Iterator<SourceRecord.Raw> fileToRecordIterator(Path file, FileFormat fileType) {
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
