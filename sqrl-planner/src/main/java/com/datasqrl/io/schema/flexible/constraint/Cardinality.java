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
import com.datasqrl.io.schema.flexible.type.ArrayType;
import com.datasqrl.io.schema.flexible.type.Type;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

@Getter
public class Cardinality implements Constraint {

  public static final Name NAME = Name.system("cardinality");

  public static final Cardinality UNCONSTRAINED = new Cardinality(0, Long.MAX_VALUE);

  private long min;
  private long max;

  private Cardinality() {} // For Kryo

  public Cardinality(long min, long max) {
    //    Preconditions.checkArgument(min >= 0);
    //    Preconditions.checkArgument(max >= min && max > 0);
    this.min = min;
    this.max = max;
  }

  public boolean isSingleton() {
    return max <= 1;
  }

  public boolean isNonZero() {
    return min > 0;
  }

  @Override
  public boolean satisfies(Object value) {
    //    Preconditions.checkArgument(value.getClass().isArray());
    long length = ((Object[]) value).length;
    return length >= min && length <= max;
  }

  @Override
  public boolean appliesTo(Type type) {
    return type instanceof ArrayType;
  }

  @Override
  public Name getName() {
    return NAME;
  }

  @Override
  public Map<String, Object> export() {
    return Map.of(Factory.KEYS[0], min, Factory.KEYS[1], max);
  }

  @Override
  public String toString() {
    return NAME.getDisplay() + "[" + min + ":" + max + "]";
  }

  public static class Factory implements Constraint.Factory {

    public static final String[] KEYS = {"min", "max"};

    @Override
    public Name getName() {
      return NAME;
    }

    @Override
    public Optional<Constraint> create(Map<String, Object> parameters, ErrorCollector errors) {
      var minmax = new long[2];
      for (var i = 0; i < minmax.length; i++) {
        var value = parameters.get(KEYS[i]);
        var v = getInt(value);
        if (v.isEmpty()) {
          errors.fatal("Invalid integer value [%s] for key [%s]", value, KEYS[i]);
          return Optional.empty();
        } else {
          minmax[i] = v.get();
        }
      }
      if (minmax[0] < 0 || minmax[1] < 1 || minmax[0] > minmax[1]) {
        errors.fatal("Invalid min [%s] and max [%s] values", minmax[0], minmax[1]);
        return Optional.empty();
      }
      return Optional.of(new Cardinality(minmax[0], minmax[1]));
    }

    public static Optional<Long> getInt(Object value) {
      if (value == null || !(value instanceof Number)) {
        return Optional.empty();
      }
      return Optional.of(((Number) value).longValue());
    }
  }
}
