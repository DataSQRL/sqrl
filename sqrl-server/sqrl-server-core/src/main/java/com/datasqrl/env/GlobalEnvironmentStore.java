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
package com.datasqrl.env;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Thread-safe global environment store that provides a centralized key-value storage for
 * environment variables throughout the JVM process.
 *
 * <p>This class uses Guava's Cache for optimal thread-safe read performance and is designed to be
 * accessed from any module in the DataSQRL project.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class GlobalEnvironmentStore {

  private static final Cache<String, String> cache = CacheBuilder.newBuilder().build();

  /**
   * Stores a key-value pair in the environment store.
   *
   * @param key the environment variable name
   * @param value the environment variable value
   * @throws IllegalArgumentException if key is null or empty
   */
  public static void put(String key, String value) {
    if (key == null || key.trim().isEmpty()) {
      throw new IllegalArgumentException("Environment variable key cannot be null or empty");
    }

    cache.put(key, value);
    log.debug("Environment variable set: {}={}", key, value);
  }

  /**
   * Retrieves a value from the environment store.
   *
   * @param key the environment variable name
   * @return the environment variable value, or null if not found
   */
  public static String get(String key) {
    if (key == null || key.trim().isEmpty()) {
      return null;
    }

    var value = cache.getIfPresent(key);
    log.debug("Environment variable retrieved: {}={}", key, value);
    return value;
  }

  /**
   * Retrieves a value from the environment store with a default fallback.
   *
   * @param key the environment variable name
   * @param defaultValue the default value to return if key is not found
   * @return the environment variable value, or defaultValue if not found
   */
  public static String getOrDefault(String key, String defaultValue) {
    var value = get(key);
    return value != null ? value : defaultValue;
  }

  /**
   * Stores multiple key-value pairs in the environment store.
   *
   * @param envMap map of environment variables to store
   */
  public static void putAll(Map<String, String> envMap) {
    if (envMap == null || envMap.isEmpty()) {
      return;
    }

    cache.putAll(envMap);
    log.debug("Environment variables set: {}", envMap.keySet());
  }

  /**
   * Returns all environment variables in the store as a map.
   *
   * @return immutable map of all environment variables
   */
  public static Map<String, String> getAll() {
    return Map.copyOf(cache.asMap());
  }

  /**
   * Checks if a key exists in the environment store.
   *
   * @param key the environment variable name
   * @return true if the key exists, false otherwise
   */
  public static boolean contains(String key) {
    if (key == null || key.trim().isEmpty()) {
      return false;
    }

    return cache.getIfPresent(key) != null;
  }

  /**
   * Clears all environment variables from the store. This method should be used with caution as it
   * affects the entire JVM process.
   */
  public static void clear() {
    cache.invalidateAll();
    log.debug("All environment variables cleared");
  }

  /**
   * Returns the current size of the environment store.
   *
   * @return number of environment variables stored
   */
  public static long size() {
    return cache.size();
  }
}
