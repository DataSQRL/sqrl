/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.io.schema.flexible.constraint;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.type.Type;
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

  // TODO: Discover Factories
  Constraint.Factory[] FACTORIES = {
    new NotNull.Factory(), new Cardinality.Factory(), new Unique.Factory()
  };

  Lookup FACTORY_LOOKUP =
      new Lookup() {

        private final Map<Name, Constraint.Factory> factoriesByName =
            Arrays.stream(FACTORIES)
                .collect(Collectors.toMap(f -> f.getName(), Function.identity()));

        @Override
        public Factory get(Name constraintName) {
          return factoriesByName.get(constraintName);
        }
      };
}
