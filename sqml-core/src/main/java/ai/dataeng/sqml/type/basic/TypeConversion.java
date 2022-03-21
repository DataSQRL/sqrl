package ai.dataeng.sqml.type.basic;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Set;

public interface TypeConversion<T> {

    /**
     * Detect if this type can be parsed from the provided string.
     * Should only return true if the provided string is unambiguously a string representation of this type.
     * @param original
     * @return
     */
    public default boolean detectType(String original) {
        return false;
    }

    /**
     * Detect if this type can be extracted from the provided map.
     * Should only return true if the provided map is unambiguously a composite representation of this type.
     * @param originalComposite
     * @return
     */
    public default boolean detectType(Map<String,Object> originalComposite) {
        return false;
    }

    /**
     * Parses the detected type out of this string or map.
     * This method is only called if {@link #detectType(String)} or {@link #detectType(Map)} returned true.
     * @param original
     * @return
     */
    public default ConversionResult<T, ProcessMessage> parseDetected(Object original) {
        Preconditions.checkArgument(original instanceof String || original instanceof Map);
        return ConversionResult.fatal("Cannot convert [%s]", original);
    }

    /**
     * Returns all the java classes that map onto this type.
     * @return
     */
    public Set<Class> getJavaTypes();

    /**
     * Casts o to the java type associated with this basic type
     * The object o can be of any java type within the type hierarchy of this basic type.
     *
     * @param o
     * @return
     */
    public default T convert(Object o) {
        return (T)o;
    }


}
