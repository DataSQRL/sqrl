package ai.datasqrl.util;

import lombok.SneakyThrows;
import lombok.Value;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface TestGraphQLSchema {

    String getName();

    Path getSchemaPath();

    @SneakyThrows
    default String getSchema() {
        return Files.readString(getSchemaPath());
    }

    Map<String,String> getQueries();

    @Value
    class Directory implements TestGraphQLSchema {

        public static final String SCHEMA_FILE = "schema.graphqls";
        public static final String QUERY_FILE_SUFFIX = ".query.graphql";

        Path schemaDir;


        @Override
        public String getName() {
            return schemaDir.getFileName().toString();
        }

        @Override
        public Path getSchemaPath() {
            return schemaDir.resolve(SCHEMA_FILE);
        }

        public static List<TestGraphQLSchema> of(Path... paths) {
            return Arrays.stream(paths).map(Directory::new).collect(Collectors.toList());
        }

        @Override
        @SneakyThrows
        public Map<String, String> getQueries() {
            Map<String, String> result = new LinkedHashMap<>();
            try (Stream<Path> files = Files.list(schemaDir).filter(Files::isRegularFile)
                    ) {
                files.forEach(f -> {
                    String filename = f.getFileName().toString();
                    if (filename.endsWith(QUERY_FILE_SUFFIX)) {
                        try {
                            result.put(filename.substring(0, filename.length() - QUERY_FILE_SUFFIX.length()),
                                    Files.readString(f));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
            return result;
        }
    }

}
