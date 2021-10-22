package ai.dataeng.sqml.schema2;


import ai.dataeng.sqml.schema2.name.Name;

import java.io.Serializable;
import java.util.Optional;

public interface Field extends Serializable {
    public Name getName();

    public default Type getType() {
        return null;
    }

    default boolean isHidden() {
        return false;
    }
}
