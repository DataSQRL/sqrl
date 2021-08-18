package ai.dataeng.sqml.source.simplefile;

import ai.dataeng.sqml.source.SourceDataset;
import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.source.SourceTable;
import ai.dataeng.sqml.source.util.SourceRecordCreator;
import ai.dataeng.sqml.time.Time;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.OffsetDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class FileTable implements SourceTable {

    private final DirectoryDataset directory;
    private final Path file;
    private final String name;

    private final FileType fileType;

    public FileTable(@Nonnull DirectoryDataset parent, @Nonnull Path file, @Nonnull String name) {
        Preconditions.checkArgument(Files.exists(file) && Files.isRegularFile(file) && Files.isReadable(file),
                "Not a readable file: %s", file);
        Preconditions.checkNotNull(supportedFile(file),"Not a supported file extension: %s", file);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Not a valid name: %s", name);
        this.directory = parent;
        this.file = file;
        this.name = name;
        this.fileType = FileType.getFileTypeFromExtension(getExtension(file));
    }

    public FileTable(DirectoryDataset parent, Path file) {
        this(parent, file, StringUtils.capitalize(FilenameUtils.removeExtension(file.getFileName().toString())));
    }

    @Override
    public SourceDataset getDataset() {
        return directory;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean hasSchema() {
        return false;
    }

    @Override
    public DataStream<SourceRecord> getDataStream(StreamExecutionEnvironment env) {
        final OffsetDateTime fileTime;
        try {
            fileTime = Time.convert(Files.getLastModifiedTime(file));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<Map<String,Object>> data = fileType.getRecords(file);
        List<SourceRecord> records = data.stream().map(m -> new SourceRecord(m,fileTime)).collect(Collectors.toList());;
        DataStreamSource<SourceRecord> stream = env.fromCollection(records);
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
