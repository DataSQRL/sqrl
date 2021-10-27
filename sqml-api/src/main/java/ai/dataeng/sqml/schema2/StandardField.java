package ai.dataeng.sqml.schema2;

import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.tree.name.Name;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

import java.util.List;

@AllArgsConstructor
@Getter
public class StandardField implements TypedField {

    private final Name name;
    private final Type type;
    private final List<Constraint> constraints;
    private final Optional<String> alias;

    @Override
    public String toString() {
        return name.getDisplay() + ":" + type.toString();
    }

    @Override
    public Field withAlias(String alias) {
        return new StandardField(name, type, constraints, Optional.of(alias));
    }
}
