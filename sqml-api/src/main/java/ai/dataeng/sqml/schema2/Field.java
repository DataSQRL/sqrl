package ai.dataeng.sqml.schema2;


import java.io.Serializable;
import java.util.Optional;
import ai.dataeng.sqml.tree.name.Name;

public interface Field extends Serializable {
    public Name getName();

    public default Type getType() {
        return null;
    }

    default boolean isHidden() {
        return false;
    }

    Field withAlias(String alias);
}
