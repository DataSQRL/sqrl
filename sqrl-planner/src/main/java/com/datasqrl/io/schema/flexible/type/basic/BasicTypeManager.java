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
package com.datasqrl.io.schema.flexible.type.basic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

public class BasicTypeManager {

  // TODO: replace by discovery pattern so that new datatype can be registered
  public static final BasicType<?>[] ALL_TYPES = {
    BooleanType.INSTANCE,
    TimestampType.INSTANCE,
    BigIntType.INSTANCE,
    DoubleType.INSTANCE,
    IntervalType.INSTANCE,
    StringType.INSTANCE
  };

  public static final Map<Class, BasicType<?>> JAVA_TO_TYPE =
      Arrays.stream(ALL_TYPES)
          .flatMap(
              t -> {
                var classes = t.conversion().getJavaTypes().stream();
                Stream<Pair<Class, BasicType>> s = classes.map(c -> Pair.of(c, t));
                return s;
              })
          .collect(Collectors.toUnmodifiableMap(Pair::getKey, Pair::getValue));

  public static final Map<String, BasicType<?>> ALL_TYPES_BY_NAME =
      Arrays.stream(ALL_TYPES)
          .flatMap(type -> type.getNames().stream().map(name -> Pair.of(name, type)))
          .collect(
              Collectors.toUnmodifiableMap(
                  t -> (t.getLeft()).toLowerCase(Locale.ENGLISH), Pair::getRight));

  public static final Map<Pair<BasicType, BasicType>, Pair<BasicType, Integer>>
      TYPE_COMBINATION_MATRIX = computeTypeCombinationMatrix();

  private static Map<Pair<BasicType, BasicType>, Pair<BasicType, Integer>>
      computeTypeCombinationMatrix() {
    Map<Pair<BasicType, BasicType>, Pair<BasicType, Integer>> map = new HashMap<>();
    for (BasicType smaller : ALL_TYPES) {
      for (BasicType larger : ALL_TYPES) {
        if (smaller.compareTo(larger) < 0) {
          // See what the distances are from casting directly from one type to the other
          BasicType combinedType = null;
          var combinedDistance = Integer.MAX_VALUE;
          Optional<Integer> smaller2Larger = larger.conversion().getTypeDistance(smaller);
          Optional<Integer> larger2Smaller = smaller.conversion().getTypeDistance(larger);
          if (smaller2Larger.isPresent() || larger2Smaller.isPresent()) {
            if (smaller2Larger.orElse(Integer.MAX_VALUE)
                < larger2Smaller.orElse(Integer.MAX_VALUE)) {
              combinedType = larger;
              combinedDistance = smaller2Larger.get();
            } else {
              combinedType = smaller;
              combinedDistance = larger2Smaller.get();
            }
          }
          // and compare to casting to a third type (for all types)
          for (BasicType third : ALL_TYPES) {
            if (third == smaller || third == larger) {
              continue;
            }
            Optional<Integer> ds = third.conversion().getTypeDistance(smaller),
                dl = third.conversion().getTypeDistance(larger);
            Optional<Integer> dist = ds.flatMap(d1 -> dl.map(d2 -> Math.max(d1, d2)));
            if (dist.orElse(Integer.MAX_VALUE) < combinedDistance) {
              combinedType = third;
              combinedDistance = dist.get();
            }
          }
          //          Preconditions.checkArgument(combinedType != null && combinedDistance <
          // Integer.MAX_VALUE);
          map.put(Pair.of(smaller, larger), Pair.of(combinedType, combinedDistance));
        }
      }
    }
    return map;
  }

  public static Optional<BasicType> combine(
      @NonNull BasicType t1, @NonNull BasicType t2, int maxTypeDistance) {
    Pair<BasicType, BasicType> key;
    var comp = t1.compareTo(t2);
    if (comp == 0) {
      return Optional.of(t1);
    } else if (comp < 0) {
      key = Pair.of(t1, t2);
    } else {
      key = Pair.of(t2, t1);
    }

    var combination = TYPE_COMBINATION_MATRIX.get(key);
    assert combination
        != null; // Otherwise the pre-computation is flawed since we can always cast to string
    if (combination.getValue() <= maxTypeDistance) {
      return Optional.of(combination.getKey());
    }
    return Optional.empty();
  }

  public static BasicType combineForced(@NonNull BasicType t1, @NonNull BasicType t2) {
    return combine(t1, t2, Integer.MAX_VALUE).get();
  }

  public static Optional<Integer> typeCastingDistance(BasicType fromType, BasicType toType) {
    if (fromType.getClass().equals(toType.getClass())) {
      return Optional.of(0);
    }
    return toType.conversion().getTypeDistance(fromType);
  }

  public static BasicType getTypeByJavaClass(Class clazz) {
    return JAVA_TO_TYPE.get(clazz);
  }

  public static BasicType getTypeByName(String name) {
    return ALL_TYPES_BY_NAME.get(name.trim().toLowerCase(Locale.ENGLISH));
  }

  public static BasicType detectType(Map<String, Object> originalComposite) {
    for (BasicType type : ALL_TYPES) {
      if (type.conversion().detectType(originalComposite)) {
        return type;
      }
    }
    return null;
  }

  public static BasicType detectType(String original) {
    for (BasicType type : ALL_TYPES) {
      if (type.conversion().detectType(original)) {
        return type;
      }
    }
    return null;
  }
}
