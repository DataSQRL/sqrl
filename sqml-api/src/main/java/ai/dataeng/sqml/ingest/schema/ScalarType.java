package ai.dataeng.sqml.ingest.schema;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum ScalarType {

    STRING("string"),
    NUMBER("number"),
    INTEGER("integer"),
    FLOAT("float"),
    DATETIME("datetime"),
    UUID("uuid"),
    BOOLEAN("boolean");

    private final String name;

    ScalarType(String name) {
        this.name = name;
    }

    private static final Map<String,ScalarType> NAME_MAPPING = Arrays.stream(values()).collect(Collectors.toUnmodifiableMap(t -> t.name, Function.identity()));

    public static ScalarType getTypeByName(String name) {
        return NAME_MAPPING.get(name.trim().toLowerCase(Locale.ENGLISH));
    }
}
