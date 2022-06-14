package ai.datasqrl.schema.constraint;

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

  boolean satisfies(Object value);

  boolean appliesTo(Type type);

  Name getName();

  Map<String, Object> export();

  interface Factory {

    Name getName();

    Optional<Constraint> create(Map<String, Object> parameters, ErrorCollector errors);

  }

  interface Lookup {

    Factory get(Name constraintName);

    default Factory get(String constraintName) {
      return get(Name.system(constraintName));
    }

  }

  //TODO: Discover Factories
  Constraint.Factory[] FACTORIES = {new NotNull.Factory(),
      new Cardinality.Factory(), new Unique.Factory()};

  Lookup FACTORY_LOOKUP = new Lookup() {

    private final Map<Name, Constraint.Factory> factoriesByName = Arrays.stream(FACTORIES)
        .collect(Collectors.toMap(f -> f.getName(), Function.identity()));

    @Override
    public Factory get(Name constraintName) {
      return factoriesByName.get(constraintName);
    }
  };

}
