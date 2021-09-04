package ai.dataeng.sqml.schema2;

import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.name.Name;
import lombok.Value;

import java.util.List;

@Value
public class Field<T extends Type,Context> {

    private final Name name;
    private final T type;
    private final Context context;
    private final List<Constraint> constraints;

    @Override
    public String toString() {
        return name.getDisplay() + ":" + type.toString();
    }

}
