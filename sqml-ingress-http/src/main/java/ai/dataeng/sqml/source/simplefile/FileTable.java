package ai.dataeng.sqml.source.simplefile;

import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.ingest.source.SourceTable;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class FileTable implements SourceTable {

    private final DirectoryDataset directory;
    private final Path file;
    private final Name name;

    private final FileType fileType;

    public FileTable(@Nonnull DirectoryDataset parent, @Nonnull Path file, @Nonnull Name name) {
        Preconditions.checkArgument(Files.exists(file) && Files.isRegularFile(file) && Files.isReadable(file),
                "Not a readable file: %s", file);
        Preconditions.checkNotNull(supportedFile(file),"Not a supported file extension: %s", file);
        this.directory = parent;
        this.file = file;
        this.name = name;
        this.fileType = FileType.getFileTypeFromExtension(getExtension(file));
    }

    public FileTable(DirectoryDataset parent, Path file) {
        this(parent, file, parent.getRegistration().toName(FilenameUtils.removeExtension(file.getFileName().toString())));
    }

    @Override
    public SourceDataset getDataset() {
        return directory;
    }

    @Override
    public Name getName() {
        return name;
    }

    @Override
    public boolean hasSchema() {
        return false;
    }

    @Override
    public DataStream<SourceRecord<String>> getDataStream(StreamExecutionEnvironment env) {
        final Instant fileTime;
        try {
            fileTime = Files.getLastModifiedTime(file).toInstant();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<Map<String,Object>> data = fileType.getRecords(file);
        List<SourceRecord<String>> records = data.stream().map(m -> new SourceRecord<String>(m,fileTime)).collect(Collectors.toList());;
        DataStreamSource<SourceRecord<String>> stream = env.fromCollection(records);
//        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SourceRecord>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getSourceTime().toEpochSecond()));
        return stream;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileTable fileTable = (FileTable) o;
        return directory.equals(fileTable.directory) && file.equals(fileTable.file) && name.equals(fileTable.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directory, file, name);
    }

    @Override
    public String toString() {
        return "FileTable{" +
                "directory=" + directory +
                ", file=" + file +
                ", name='" + name + '\'' +
                '}';
    }

    private static final String getExtension(Path p) {
        return FilenameUtils.getExtension(p.getFileName().toString());
    }

    public static final boolean supportedFile(Path p) {
        return FileType.validExtension(getExtension(p));
    }
}
