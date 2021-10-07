package ai.dataeng.sqml.schema2;


import ai.dataeng.sqml.schema2.name.Name;
import java.util.Optional;

public interface Field {

    static Field newDataField(String suffix, Type type) {
        return null;
    }

    static Field newUnqualified(Name name, Type type) {
        return null;
    }

    static <U> Field newUnqualified(Optional<U> u, Type orElseThrow, boolean present) {
        return null;
    }

    static Field newQualified(String relationAlias, Object o, Type type) {
        return null;
    }

    public Name getName();

    public default Type getType() {
        return null;
    }

    default boolean isHidden() {
        return false;
    }
}
