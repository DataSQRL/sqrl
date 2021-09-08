package ai.dataeng.sqml.schema2.constraint;

import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.basic.ConversionResult;
import ai.dataeng.sqml.schema2.basic.SimpleConversionError;
import ai.dataeng.sqml.schema2.name.Name;

import java.util.Map;

public class NotNull implements Constraint {

    public static final Name NAME = Name.system("not_null");

    private static final NotNull INSTANCE = new NotNull();

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

    public static class Factory implements Constraint.Factory {

        @Override
        public Name getName() {
            return NAME;
        }

        @Override
        public ConversionResult<Constraint, ConversionError> create(Map<String, Object> parameters) {
            return ConversionResult.of(INSTANCE);
        }

    }

}
