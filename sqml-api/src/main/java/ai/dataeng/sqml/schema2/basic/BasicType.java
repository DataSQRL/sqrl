package ai.dataeng.sqml.schema2.basic;

import ai.dataeng.sqml.schema2.Type;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface BasicType extends Type {

    String getName();

    //TODO: replace by discovery pattern so that new datatype can be registered
    public static final BasicType[] ALL_TYPES = {BooleanType.INSTANCE, StringType.INSTANCE,
                                                 NumberType.INSTANCE, IntegerType.INSTANCE, FloatType.INSTANCE,
                                                 DateTimeType.INSTANCE, UuidType.INSTANCE
    };

    public static final Map<String,BasicType> ALL_TYPES_BY_NAME = Arrays.stream(ALL_TYPES)
            .collect(Collectors.toUnmodifiableMap(t -> t.getName().trim().toLowerCase(Locale.ENGLISH), Function.identity()));

    public static BasicType getTypeByName(String name) {
        return ALL_TYPES_BY_NAME.get(name.trim().toLowerCase(Locale.ENGLISH));
    }

}
