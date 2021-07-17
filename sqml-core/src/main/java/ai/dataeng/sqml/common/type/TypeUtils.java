/*
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
package ai.dataeng.sqml.common.type;

import static ai.dataeng.sqml.common.type.DoubleType.DOUBLE;
import static ai.dataeng.sqml.common.type.IntegerType.INTEGER;
import static ai.dataeng.sqml.common.type.TinyintType.TINYINT;
import static ai.dataeng.sqml.common.type.BigintType.BIGINT;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class TypeUtils {

  public static final int NULL_HASH_CODE = 0;

  private TypeUtils() {
  }

  public static boolean isNumericType(Type type) {
    return isNonDecimalNumericType(type) || type instanceof DecimalType;
  }

  public static boolean isNonDecimalNumericType(Type type) {
    return isExactNumericType(type) || isApproximateNumericType(type);
  }

  public static boolean isExactNumericType(Type type) {
    return type.equals(BIGINT) || type.equals(INTEGER) || type
        .equals(TINYINT);
  }

  public static boolean isApproximateNumericType(Type type) {
    return type.equals(DOUBLE) ;
  }

  static void validateEnumMap(Map<String, ?> enumMap) {
    if (enumMap.containsKey(null)) {
      throw new IllegalArgumentException("Enum cannot contain null key");
    }
    int nUniqueAndNotNullValues = enumMap.values().stream()
        .filter(Objects::nonNull).collect(toSet()).size();
    if (nUniqueAndNotNullValues != enumMap.size()) {
      throw new IllegalArgumentException("Enum cannot contain null or duplicate values");
    }
    int nCaseInsensitiveKeys = enumMap.keySet().stream().map(k -> k.toUpperCase(ENGLISH))
        .collect(Collectors.toSet()).size();
    if (nCaseInsensitiveKeys != enumMap.size()) {
      throw new IllegalArgumentException("Enum cannot contain case-insensitive duplicate keys");
    }
  }

  static <V> Map<String, V> normalizeEnumMap(Map<String, V> entries) {
    return entries.entrySet().stream()
        .collect(toMap(e -> e.getKey().toUpperCase(ENGLISH), Map.Entry::getValue));
  }
}
