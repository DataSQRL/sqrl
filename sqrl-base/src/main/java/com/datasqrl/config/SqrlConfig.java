package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public interface SqrlConfig {

  public SqrlConfig getSubConfig(String name);
  public boolean hasSubConfig(String name);

  /**
   * Returns the keys that are at the local level in the configuration
   * and deduplicates keys that occur multiple times (because they have multiple sub-keys).
   *
   * @return Iterable over all local keys at the current level of nesting in the configuration
   */
  Iterable<String> getKeys();

  /**
   * Returns all keys in this (sub) configuration including
   * nested keys (e.g. "some.nested.config").
   *
   * @return Iterable over all keys in this configuration
   */
//  Iterable<String> getAllKeys();

  boolean containsKey(String key);

  <T> Value<T> as(String key, Class<T> clazz);

  <T> Value<T> allAs(Class<T> clazz);

  <T> Value<List<T>> asList(String key, Class<T> clazz);

  <T> Value<LinkedHashMap<String,T>> asMap(String key, Class<T> clazz);

  ErrorCollector getErrorCollector();

  void setProperty(String key, Object value);

  void setProperties(Object value);

  void copy(SqrlConfig from);

  default void toFile(Path file) {
    toFile(file,false);
  }

  void toFile(Path file, boolean pretty);

  Map<String, Object> toMap();

  Map<String, String> toStringMap();

  SerializedSqrlConfig serialize(); //TODO: add secrets injector

  default Value<String> asString(String key) {
    return as(key, String.class).map(String::trim);
  }

  default Value<Long> asLong(String key) {
    return as(key, Long.class);
  }

  default Value<Integer> asInt(String key) {
    return as(key, Integer.class);
  }

  default Value<Boolean> asBool(String key) {
    return as(key, Boolean.class);
  }

  boolean hasKey(String profiles);

  interface Value<T> {

    T get();

    default Optional<T> getOptional() {
      T value = this.withDefault(null).get();
      if (value==null) return Optional.empty();
      else return Optional.of(value);
    }

    Value<T> withDefault(T defaultValue);

    Value<T> validate(Predicate<T> validator, String msg);

    Value<T> map(Function<T,T> mapFunction);

  }

  static SqrlConfig create() {
    return create(ErrorCollector.root());
  }

  static SqrlConfig create(ErrorCollector errors) {
    return SqrlConfigCommons.create(errors);
  }

  static SqrlConfig EMPTY = create();

}
