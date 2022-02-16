package ai.dataeng.sqml.io.sources.impl.file;

import ai.dataeng.sqml.io.sources.util.SourceRecordCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public enum FileType {

    CSV("csv") {
        @Override
        public List<Map<String,Object>> getRecords(Path file) {
            try {
                final CSVReader reader = new CSVReaderBuilder(new FileReader(file.toFile())).build();
                final String[] header = reader.readNext();
                Preconditions.checkNotNull(header,"Invalid CSV file without header: %s", file);
                List<Map<String,Object>> records = new ArrayList<>();
                String[] nextRecord;
                while ((nextRecord = reader.readNext())!=null) {
                    records.add(SourceRecordCreator.dataFrom(header,nextRecord));
                }
                return records;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    },
    JSON("json") {
        @Override
        public List<Map<String,Object>> getRecords(Path file) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                List<?> records =  mapper.readValue(file.toFile(), List.class);
                return (List)records;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private final String[] extensions;

    FileType(String[] extensions) {
        this.extensions = extensions;
    }

    FileType(String extension) {
        this(new String[]{extension});
    }

    public String[] getExtensions() {
        return extensions;
    }

    public abstract List<Map<String,Object>> getRecords(Path file);


    private static final Map<String, FileType> EXTENSION_MAP = Arrays.stream(values()).flatMap(ft -> Arrays.stream(ft.extensions).map(e -> Pair.of(ft, e)))
            .collect(Collectors.toMap(Pair::getRight, Pair::getLeft));

    public static boolean validExtension(String extension) {
        return EXTENSION_MAP.containsKey(extension);
    }


    public static FileType getFileTypeFromExtension(String extension) {
        FileType ft = EXTENSION_MAP.get(extension);
        return ft;
    }
}
