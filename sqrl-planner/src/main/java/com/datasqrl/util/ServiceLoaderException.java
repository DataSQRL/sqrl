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

import java.util.List;

public class ServiceLoaderException extends RuntimeException {

  final Class<?> clazz;
  final List<String> identifiers;

  public ServiceLoaderException(Class<?> clazz, String identifier) {
    this(clazz, List.of(identifier));
  }

  public ServiceLoaderException(Class<?> clazz, List<String> identifiers) {
    super(
        "Could not load %s dependency for identifier(s): %s"
            .formatted(clazz.getSimpleName(), String.join(",", identifiers)));
    this.clazz = clazz;
    this.identifiers = identifiers;
  }
}
