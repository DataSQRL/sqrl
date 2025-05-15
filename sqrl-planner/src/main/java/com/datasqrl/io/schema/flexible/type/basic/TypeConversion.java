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

import com.datasqrl.error.ErrorCollector;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface TypeConversion<T> {

  /**
   * Detect if this type can be parsed from the provided string. Should only return true if the
   * provided string is unambiguously a string representation of this type.
   *
   * @param original
   * @return
   */
  default boolean detectType(String original) {
    return false;
  }

  /**
   * Detect if this type can be extracted from the provided map. Should only return true if the
   * provided map is unambiguously a composite representation of this type.
   *
   * @param originalComposite
   * @return
   */
  default boolean detectType(Map<String, Object> originalComposite) {
    return false;
  }

  /**
   * Parses the detected type out of this string or map. This method is only called if {@link
   * #detectType(String)} or {@link #detectType(Map)} returned true.
   *
   * @param original
   * @return
   */
  default Optional<T> parseDetected(Object original, ErrorCollector errors) {
    //    Preconditions.checkArgument(original instanceof String || original instanceof Map);
    errors.fatal("Cannot convert [%s]", original);
    return Optional.empty();
  }

  /**
   * Returns all the java classes that map onto this type.
   *
   * @return
   */
  Set<Class> getJavaTypes();

  /**
   * Casts o to the java type associated with this basic type The object o can be of any java type
   * within the type hierarchy of this basic type.
   *
   * @param o
   * @return
   */
  default T convert(Object o) {
    return (T) o;
  }

  /**
   * If the other type can be cast to this type, this method returns the distance between the types.
   * This value is positive if it is an up-casting (i.e. no information is lost but may be
   * compressed) and negative if it is a down-casting (i.e. information is lost).
   *
   * <p>The distance is a measure of how much compression and space expansion (in case of a positive
   * number) or information loss (in case of a negative number) happens upon casting to guide the
   * optimizer to pick most "compatible" types, i.e. those with the shortest distance.
   *
   * <p>It is assumed that the type distance between identical types is 0 and this method should not
   * return a value that contradicts that (but may return empty in that case).
   *
   * @param fromType
   * @return
   */
  Optional<Integer> getTypeDistance(BasicType fromType);
}
