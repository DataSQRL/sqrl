package ai.dataeng.sqml.type;

import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.constraint.Constraint;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class StandardField implements TypedField {

    private final Name name;
    private final Type type;
    private final List<Constraint> constraints;
    private final Optional<QualifiedName> alias;

    @Override
    public String toString() {
        return name.getDisplay() + ":" + type.toString();
    }
}
