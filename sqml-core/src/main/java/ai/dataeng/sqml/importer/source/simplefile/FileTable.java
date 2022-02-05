package ai.dataeng.sqml.importer.source.simplefile;

import ai.dataeng.sqml.execution.flink.ingest.schema.FlexibleDatasetSchema.FlexibleField;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlexibleDatasetSchema.TableField;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceDataset;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceRecord;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceTable;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.basic.BooleanType;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.type.basic.FloatType;
import ai.dataeng.sqml.type.basic.IntegerType;
import ai.dataeng.sqml.type.basic.StringType;
import ai.dataeng.sqml.type.basic.UuidType;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.types.Row;

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
    public DataStream<Row> getDataStream2(StreamExecutionEnvironment env,
        List<FlexibleField> fields) {
        final Instant fileTime;
        try {
            fileTime = Files.getLastModifiedTime(file).toInstant();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<Map<String,Object>> data = fileType.getRecords(file);

        List<Row> records = data.stream().map(m ->{
            Object[] objects = new Object[fields.size() + 1];
            for (int i = 0; i < fields.size(); i++) {
                FlexibleField field = fields.get(i);
                objects[i] = m.get(field.getCanonicalName());
            }

            objects[fields.size()] = UUID.randomUUID();

            return Row.of(objects);
        }).collect(Collectors.toList());

        return env.fromCollection(records)
            .returns(toTypeInformation(fields));
//        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SourceRecord>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getSourceTime().toEpochSecond()));
//        return stream;
    }

    private TypeInformation<Row> toTypeInformation(List<FlexibleField> fieldList) {
        List<String> names = new ArrayList<>();
        List<TypeInformation> types = new ArrayList<>();
        for (int i = 0; i < fieldList.size(); i++) {
            FlexibleField field = fieldList.get(i);
            if (field.getType() instanceof RelationType) {
                continue;
            }
            names.add(field.getCanonicalName());
            if (this.fileType == FileType.CSV) {
                types.add(Types.STRING);//toType(field.getTypes().get(0).getType());
            } else {
                types.add(toType(field.getTypes().get(0).getType()));
            }
        }
        names.add("__uuid");
        types.add(TypeInformation.of(UUID.class));

        return Types.ROW_NAMED(
            names.toArray(new String[0]),
            types.toArray(new TypeInformation[0]));
    }

    private TypeInformation toType(Type type) {
        return type.accept(new SqmlTypeVisitor<>() {
            @Override
            public TypeInformation visitBooleanType(BooleanType type, Object context) {
                return Types.BOOLEAN;
            }

            @Override
            public TypeInformation visitFloatType(FloatType type, Object context) {
                return Types.FLOAT;
            }

            @Override
            public TypeInformation visitIntegerType(IntegerType type, Object context) {
                return Types.INT;
            }

            @Override
            public TypeInformation visitStringType(StringType type, Object context) {
                return Types.STRING;
            }

            @Override
            public TypeInformation visitDateTimeType(DateTimeType type, Object context) {
                return Types.INSTANT;
            }

            @Override
            public TypeInformation visitUuidType(UuidType type, Object context) {
                return TypeInformation.of(UUID.class);
            }
        }, null);
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
