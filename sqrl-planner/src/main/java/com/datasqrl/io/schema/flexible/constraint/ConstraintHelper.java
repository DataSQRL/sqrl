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

import java.util.Optional;

public class ConstraintHelper {

  public static boolean isNonNull(Iterable<Constraint> constraints) {
    return getConstraint(constraints, NotNull.class).isPresent();
  }

  public static Cardinality getCardinality(Iterable<Constraint> constraints) {
    return ConstraintHelper.getConstraint(constraints, Cardinality.class)
        .orElse(Cardinality.UNCONSTRAINED);
  }

  public static <C extends Constraint> Optional<C> getConstraint(
      Iterable<Constraint> contraints, Class<C> constraintClass) {
    for (Constraint c : contraints) {
      if (constraintClass.isInstance(c)) {
        return Optional.of((C) c);
      }
    }
    return Optional.empty();
  }
}
