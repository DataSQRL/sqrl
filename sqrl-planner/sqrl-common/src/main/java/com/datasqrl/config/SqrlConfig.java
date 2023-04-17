package com.datasqrl.config;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public interface SqrlConfig {

  public SqrlConfigCommons getSubConfig(String name);

  Iterable<String> getKeys();

  <T> Value<T> key(String key, Class<T> clazz);

  <T> Value<List<T>> keyList(String key, Class<T> clazz);

  <T> Value<Map<String,T>> keyMap(String key, Class<T> clazz);

  void setProperty(String key, Object value);

  void toFile(Path file);

  default Value<String> keyString(String key) {
    return key(key, String.class).map(String::trim);
  }

  default Value<Long> keyLong(String key) {
    return key(key, Long.class);
  }

  default Value<Integer> keyInt(String key) {
    return key(key, Integer.class);
  }

  default Value<Boolean> keyBool(String key) {
    return key(key, Boolean.class);
  }

  interface Value<T> {

    T get();

    Value<T> withDefault(T defaultValue);

    Value<T> validate(Predicate<T> validator, String msg);

    Value<T> map(Function<T,T> mapFunction);

  }

}
