package com.datasqrl.function.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.SneakyThrows;

public class ServiceLoaderDiscovery {

  private static final Cache<Class<?>, List<?>> cache = CacheBuilder.newBuilder().maximumSize(10_000)
      .build();

  public static <L> Optional<L> findFirst(@NonNull Class<L> clazz,
      @NonNull Predicate<L> condition) {
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
    return (List<L>) cache.get(clazz, () -> {
      List<L> loaded = new ArrayList<>();
      ServiceLoader.load(clazz).forEach(loaded::add);
      return loaded;
    });
  }

  public static <L> L get(@NonNull Class<L> clazz, @NonNull Predicate<L> condition,
      @NonNull List<String> identifiers) {
    return findFirst(clazz, condition).orElseThrow(
        () -> new RuntimeException("Could not find server loader class:" + clazz.getName()));
  }

  public static <L> Optional<L> findFirst(@NonNull Class<L> clazz, @NonNull Function<L, String> key,
      @NonNull String value) {
    return findFirst(clazz, l -> key.apply(l).equalsIgnoreCase(value));
  }

  public static <L> L get(@NonNull Class<L> clazz, @NonNull Function<L, String> key,
      @NonNull String value) {
    return findFirst(clazz, key, value)
        .orElseThrow(() -> new RuntimeException("Could not find server loader class:" + clazz.getName()));
  }

  public static <L> Optional<L> findFirst(@NonNull Class<L> clazz,
      @NonNull Function<L, String> key1, @NonNull String value1, @NonNull Function<L, String> key2,
      @NonNull String value2) {
    return findFirst(clazz,
        l -> key1.apply(l).equalsIgnoreCase(value1) && key2.apply(l).equalsIgnoreCase(value2));
  }

  public static <L> L get(@NonNull Class<L> clazz, @NonNull Function<L, String> key1,
      @NonNull String value1, @NonNull Function<L, String> key2, @NonNull String value2) {
    return findFirst(clazz, key1, value1, key2, value2).orElseThrow(
        () -> new RuntimeException("Could not find server loader class:" + clazz.getName()));
  }


}
