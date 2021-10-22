package ai.dataeng.sqml.schema2.constraint;

import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.basic.ConversionResult;
import ai.dataeng.sqml.schema2.basic.SimpleConversionError;
import ai.dataeng.sqml.schema2.name.Name;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface Constraint extends Serializable {

    public boolean satisfies(Object value);

    public boolean appliesTo(Type type);

    public Name getName();

    public Map<String,Object> export();

    public interface Factory {

        Name getName();

        ConversionResult<Constraint, ConversionError> create(Map<String,Object> parameters);

    }

    public interface Lookup {

        public Factory get(Name constraintName);

        public default Factory get(String constraintName) {
            return get(Name.system(constraintName));
        }

    }

    //TODO: Discover Factories
    public static final Constraint.Factory[] FACTORIES = {new NotNull.Factory(), new Cardinality.Factory()};

    public static final Lookup FACTORY_LOOKUP = new Lookup() {

        private final Map<Name,Constraint.Factory> factoriesByName = Arrays.stream(FACTORIES).collect(Collectors.toMap(f -> f.getName(), Function.identity()));

        @Override
        public Factory get(Name constraintName) {
            return factoriesByName.get(constraintName);
        }
    };

}
