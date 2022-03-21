package ai.dataeng.sqml.type.constraint;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.basic.ConversionResult;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import java.util.Map;

public class NotNull implements Constraint {

    public static final Name NAME = Name.system("not_null");

    public static final NotNull INSTANCE = new NotNull();

    private NotNull() {
    }

    @Override
    public boolean satisfies(Object value) {
        if (value == null) return false;
        if (value.getClass().isArray()) {
            for (Object v : (Object[])value) {
                if (value == null) return false;
            }
        }
        return true;
    }

    @Override
    public boolean appliesTo(Type type) {
        return true;
    }

    @Override
    public String toString() {
        return NAME.getDisplay();
    }

    @Override
    public Name getName() {
        return NAME;
    }

    @Override
    public Map<String, Object> export() {
        return null;
    }


    public static class Factory implements Constraint.Factory {

        @Override
        public Name getName() {
            return NAME;
        }

        @Override
        public ConversionResult<Constraint, ProcessMessage> create(Map<String, Object> parameters) {
            return ConversionResult.of(INSTANCE);
        }

    }

}
