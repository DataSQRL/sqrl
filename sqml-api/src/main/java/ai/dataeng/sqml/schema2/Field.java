package ai.dataeng.sqml.schema2;


import ai.dataeng.sqml.schema2.name.Name;
import java.util.Optional;

public interface Field {
    public Name getName();

    public default Type getType() {
        return null;
    }

    default boolean isHidden() {
        return false;
    }
}
