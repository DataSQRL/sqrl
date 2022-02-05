package ai.dataeng.sqml.type.constraint;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.ArrayType;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.basic.ConversionResult;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

@Getter
public class Unique implements Constraint {

    public static final Name NAME = Name.system("unique");

    public static final Unique UNCONSTRAINED = new Unique();

    private Unique() {} //For Kryo

    @Override
    public boolean satisfies(Object value) {
        return true;
    }

    @Override
    public boolean appliesTo(Type type) {
        return false;
    }

    @Override
    public Name getName() {
        return NAME;
    }

    @Override
    public Map<String, Object> export() {
        return Map.of();
    }

    @Override
    public String toString() {
        return NAME.getDisplay();
    }

    public static class Factory implements Constraint.Factory {

        @Override
        public Name getName() {
            return NAME;
        }

        @Override
        public ConversionResult<Constraint, ProcessMessage> create(Map<String, Object> parameters) {
            return ConversionResult.of(new Unique());
        }
    }
}
