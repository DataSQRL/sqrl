package ai.dataeng.sqml.schema2;

import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.name.Name;
import lombok.Value;

import java.util.List;

@Value
public class StandardField implements Field {

    private final Name name;
    private final Type type;
    private final List<Constraint> constraints;

    @Override
    public String toString() {
        return name.getDisplay() + ":" + type.toString();
    }


}
