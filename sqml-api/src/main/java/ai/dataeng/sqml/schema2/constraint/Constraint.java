package ai.dataeng.sqml.schema2.constraint;

import ai.dataeng.sqml.schema2.Type;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface Constraint {

    public boolean satisfies(Object value);

    public boolean appliesTo(Type type);

    public interface Factory {

        String getName();

        Constraint create(Map<String,Object> parameters);

    }

    public interface Lookup {

        public Factory get(String constraintName);

    }

    //TODO: Discover Factories
    public static final Constraint.Factory[] FACTORIES = {new NotNull.Factory()};

    public static final Lookup FACTORY_LOOKUP = new Lookup() {

        private final Map<String,Constraint.Factory> factoriesByName = Arrays.stream(FACTORIES).collect(Collectors.toMap(f -> f.getName().trim().toLowerCase(Locale.ENGLISH), Function.identity()));

        @Override
        public Factory get(String constraintName) {
            return factoriesByName.get(constraintName.trim().toLowerCase(Locale.ENGLISH));
        }
    };

}
