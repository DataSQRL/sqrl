package ai.dataeng.sqml.schema2.constraint;

import ai.dataeng.sqml.schema2.Type;

import java.util.Map;

public class NotNull implements Constraint {

    public static final String NAME = "not_null";

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
        return NAME;
    }

    public static class Factory implements Constraint.Factory {

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Constraint create(Map<String, Object> parameters) {
            return INSTANCE;
        }

    }

}
