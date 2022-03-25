package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.io.sources.impl.file.FilePath;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

public enum FileFormat {

    CSV("csv") {
        @Override
        public Format getImplementation() {
            return new CSVFormat();
        }
    },
    JSON("json") {
        @Override
        public Format getImplementation() {
            return new JsonLineFormat();
        }
    };

    private final String[] identifiers;

    FileFormat(String... identifiers) {
        this.identifiers = identifiers;
    }

    public String[] getIdentifiers() {
        return identifiers;
    }

    public abstract Format getImplementation();

    private static final Map<String, FileFormat> EXTENSION_MAP = Arrays.stream(values()).flatMap(ft -> Arrays.stream(ft.identifiers).map(e -> Pair.of(ft, e)))
            .collect(Collectors.toMap(Pair::getRight, Pair::getLeft));

    public static boolean validFormat(String format) {
        return EXTENSION_MAP.containsKey(normalizeFormat(format));
    }

    public static FileFormat getFormat(String format) {
        FileFormat ft = EXTENSION_MAP.get(normalizeFormat(format));
        return ft;
    }

    private static final String normalizeFormat(String format) {
        return format.trim().toLowerCase();
    }
}
