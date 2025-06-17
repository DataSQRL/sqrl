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
package com.datasqrl.util;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;

@UtilityClass
public class StreamUtil {

  public static <T, C> Stream<C> filterByClass(Stream<T> stream, Class<C> clazz) {
    return stream.filter(clazz::isInstance).map(clazz::cast);
  }

  public static <T, C> Stream<C> filterByClass(Collection<T> col, Class<C> clazz) {
    return col.stream().filter(clazz::isInstance).map(clazz::cast);
  }

  public static <T> Optional<T> getOnlyElement(Stream<T> stream) {
    var elements = new AtomicReference<T>(null);
    var count =
        stream
            .map(
                e -> {
                  elements.set(e);
                  return e;
                })
            .count();
    if (count == 0) {
      return Optional.empty();
    } else if (count == 1) {
      return Optional.of(elements.get());
    } else {
      throw new IllegalArgumentException("Stream contains [" + count + "] elements");
    }
  }
}
