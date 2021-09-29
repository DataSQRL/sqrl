package ai.dataeng.sqml.schema2.basic;

import com.google.common.base.Preconditions;
import lombok.NonNull;

import java.util.Map;
import java.util.NoSuchElementException;
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

    public Object cast2Parent(@NonNull T o);

    public Set<Class> getJavaTypes();

    public default T convert(Object o) {
        return (T)o;
    }

    public static Object cast2Ancestor(@NonNull Object o, @NonNull BasicType from, @NonNull BasicType to) {
        if (to.equals(BasicType.ROOT_TYPE)) return o.toString(); //only when forced
        BasicType parent = from;
        while (!parent.equals(to)) {
            o = parent.conversion().cast2Parent(o);
            parent = parent.parentType();
            Preconditions.checkNotNull(parent, "[%s] is not an ancestor of [%s]", to, from);
        }
        return o;
    }


}
