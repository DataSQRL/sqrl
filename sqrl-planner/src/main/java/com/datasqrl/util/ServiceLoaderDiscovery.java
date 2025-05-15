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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.NonNull;
import lombok.SneakyThrows;

public class ServiceLoaderDiscovery {

  private static final Cache<Class<?>, List<?>> cache =
      CacheBuilder.newBuilder().maximumSize(10_000).build();

  public static <L> Optional<L> findFirst(
      @NonNull Class<L> clazz, @NonNull Predicate<L> condition) {
    List<L> services = getAll(clazz);
    for (L service : services) {
      if (condition.test(service)) {
        return Optional.of(service);
      }
    }
    return Optional.empty();
  }

  @SneakyThrows
  public static <L> List<L> getAll(Class<L> clazz) {
    return (List<L>)
        cache.get(
            clazz,
            () -> {
              List<L> loaded = new ArrayList<>();
              ServiceLoader.load(clazz).forEach(loaded::add);
              return loaded;
            });
  }

  public static <L> L get(
      @NonNull Class<L> clazz, @NonNull Predicate<L> condition, @NonNull List<String> identifiers) {
    return findFirst(clazz, condition)
        .orElseThrow(() -> new ServiceLoaderException(clazz, identifiers));
  }

  public static <L> Optional<L> findFirst(
      @NonNull Class<L> clazz, @NonNull Function<L, String> key, @NonNull String value) {
    return findFirst(clazz, l -> key.apply(l).equalsIgnoreCase(value));
  }

  public static <L> L get(
      @NonNull Class<L> clazz, @NonNull Function<L, String> key, @NonNull String value) {
    return findFirst(clazz, key, value).orElseThrow(() -> new ServiceLoaderException(clazz, value));
  }

  public static <L> Optional<L> findFirst(
      @NonNull Class<L> clazz,
      @NonNull Function<L, String> key1,
      @NonNull String value1,
      @NonNull Function<L, String> key2,
      @NonNull String value2) {
    return findFirst(
        clazz,
        l -> key1.apply(l).equalsIgnoreCase(value1) && key2.apply(l).equalsIgnoreCase(value2));
  }

  public static <L> L get(
      @NonNull Class<L> clazz,
      @NonNull Function<L, String> key1,
      @NonNull String value1,
      @NonNull Function<L, String> key2,
      @NonNull String value2) {
    return findFirst(clazz, key1, value1, key2, value2)
        .orElseThrow(() -> new ServiceLoaderException(clazz, List.of(value1, value2)));
  }
}
