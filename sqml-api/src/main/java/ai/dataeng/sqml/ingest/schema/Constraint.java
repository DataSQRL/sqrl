package ai.dataeng.sqml.ingest.schema;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface Constraint {

    public boolean satisfies(Object value);

    public boolean isTableConstraint();

    public boolean isScalarConstraint();

    public interface Factory {

        String getName();

        Constraint create(Map<String,Object> parameters);

    }

    public static final Constraint NON_NULL = new Constraint() {
        @Override
        public boolean satisfies(Object value) {
            return value!=null;
        }

        @Override
        public boolean isTableConstraint() {
            return true;
        }

        @Override
        public boolean isScalarConstraint() {
            return true;
        }
    };

    public static final Constraint.Factory NON_NULL_FACTORY = new Factory() {
        @Override
        public String getName() {
            return "not_null";
        }

        @Override
        public Constraint create(Map<String, Object> parameters) {
            return NON_NULL;
        }
    };

    public interface Lookup {

        public Factory get(String constraintName);

    }

    public static final Constraint.Factory[] FACTORIES = {NON_NULL_FACTORY};

    public static final Lookup FACTORY_LOOKUP = new Lookup() {

        private final Map<String,Constraint.Factory> factoriesByName = Arrays.stream(FACTORIES).collect(Collectors.toMap(f -> f.getName().trim().toLowerCase(Locale.ENGLISH), Function.identity()));

        @Override
        public Factory get(String constraintName) {
            return factoriesByName.get(constraintName.trim().toLowerCase(Locale.ENGLISH));
        }
    };

}
