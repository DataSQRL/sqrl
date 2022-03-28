package ai.dataeng.sqml.type.constraint;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.Type;

import java.util.Map;
import java.util.Optional;

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
        public Optional<Constraint> create(Map<String, Object> parameters, ErrorCollector errors) {
            return Optional.of(INSTANCE);
        }

    }

}
