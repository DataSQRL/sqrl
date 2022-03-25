package ai.dataeng.sqml.execution.flink.ingest;

import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.formats.FileFormat;
import ai.dataeng.sqml.io.sources.formats.Format;
import ai.dataeng.sqml.io.sources.formats.TextLineFormat;
import ai.dataeng.sqml.io.sources.impl.InputPreview;
import ai.dataeng.sqml.io.sources.impl.file.FilePath;
import ai.dataeng.sqml.io.sources.impl.file.FileSource;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@AllArgsConstructor
public class DataStreamProvider {


    public DataStream<SourceRecord.Raw> getDataStream(SourceTable table, StreamExecutionEnvironment env) {
        DataSource source = table.getDataset().getSource();
        if (source instanceof FileSource) {
            FileSource filesource = (FileSource)source;
            SourceTableConfiguration tblConfig = table.getConfiguration();
            InputPreview preview = new InputPreview(filesource,tblConfig);
            TextLineFormat.Parser parser = (TextLineFormat.Parser)tblConfig.getFormatParser();

            List<SourceRecord.Raw> allItems = preview.getTextPreview().flatMap( br -> br.lines()).map(parser::parse)
                    .map(r -> new SourceRecord.Raw(r.getRecord(), r.getSource_time())).collect(Collectors.toList());
            DataStreamSource<SourceRecord.Raw> stream = env.fromCollection(allItems);
//        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SourceRecord>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getSourceTime().toEpochSecond()));
            return stream;
        } else throw new UnsupportedOperationException("Unrecognized source table type: " + table);
    }

}
