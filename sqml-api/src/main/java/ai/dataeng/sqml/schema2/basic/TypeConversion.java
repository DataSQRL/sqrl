package ai.dataeng.sqml.schema2.basic;

import java.util.Map;
import java.util.Set;

public interface TypeConversion<T> {

    public default boolean isInferredType(String original) {
        return false;
    }

    public default boolean isInferredType(Map<String,Object> originalComposite) {
        return false;
    }

    public default ConversionResult<T, ConversionError> parse(Object original) {
        return ConversionResult.fatal("Cannot convert [%s]", original);
    }

    public Set<Class> getJavaTypes();

    public default T convert(Object o) {
        return (T)o;
    }

}
