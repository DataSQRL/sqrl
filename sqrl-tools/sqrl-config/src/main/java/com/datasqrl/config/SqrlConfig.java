package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Interface for accessing configuration files that provides convenience methods
 * for accessing different data types and methods to handle errors in a way that
 * preserves the error locality so it is easy for users to understand where the
 * error comes from and how to fix it.
 */
interface SqrlConfig {

  static final int CURRENT_VERSION = 1;
  static final String VERSION_KEY = "version";


  /**
   * All SQRL configuration files are versioned. This returns the version.
   *
   * @return the version of this configuration
   */
  int getVersion();

  SqrlConfig getSubConfig(String name);

  boolean hasSubConfig(String name);

  /**
   * Throws an error if the sub configuration does not exist or is empty
   * @param name
   */
  void validateSubConfig(String name);

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

  SqrlConfig setProperty(String key, Object value);

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

  boolean hasKey(String key);

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

  static<T extends Enum<T>> T getEnum(Value<String> value, Class<T> clazz, Optional<T> defaultValue) {
    if (defaultValue.isPresent()) value = value.withDefault(defaultValue.get().name());
    return Enum.valueOf(clazz,value.map(String::toLowerCase).validate(v -> isEnumValue(v,clazz),
        String.format("Use one of: %s",clazz.getEnumConstants())).get());
  }

  static<T extends Enum<T>> boolean isEnumValue(String value, Class<T> clazz) {
    for (T e : clazz.getEnumConstants()) {
      if(e.name().equals(value)) { return true; }
    }
    return false;
  }

  static SqrlConfig createCurrentVersion() {
    return createCurrentVersion(ErrorCollector.root());
  }

  static SqrlConfig createCurrentVersion(ErrorCollector errors) {
    return create(errors, CURRENT_VERSION);
  }

  static SqrlConfig create(SqrlConfig other) {
    return create(other.getErrorCollector(), other.getVersion());
  }

  static SqrlConfig create(ErrorCollector errors, int version) {
    return SqrlConfigCommons.create(errors, version);
  }

}
