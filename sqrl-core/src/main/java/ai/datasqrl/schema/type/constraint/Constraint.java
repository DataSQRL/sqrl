package ai.datasqrl.schema.type.constraint;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.type.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface Constraint extends Serializable {

    public boolean satisfies(Object value);

    public boolean appliesTo(Type type);

    public Name getName();

    public Map<String,Object> export();

    public interface Factory {

        Name getName();

        Optional<Constraint> create(Map<String,Object> parameters, ErrorCollector errors);

    }

    public interface Lookup {

        public Factory get(Name constraintName);

        public default Factory get(String constraintName) {
            return get(Name.system(constraintName));
        }

    }

    //TODO: Discover Factories
    public static final Constraint.Factory[] FACTORIES = {new NotNull.Factory(), new Cardinality.Factory(), new Unique.Factory()};

    public static final Lookup FACTORY_LOOKUP = new Lookup() {

        private final Map<Name,Constraint.Factory> factoriesByName = Arrays.stream(FACTORIES).collect(Collectors.toMap(f -> f.getName(), Function.identity()));

        @Override
        public Factory get(Name constraintName) {
            return factoriesByName.get(constraintName);
        }
    };

}
