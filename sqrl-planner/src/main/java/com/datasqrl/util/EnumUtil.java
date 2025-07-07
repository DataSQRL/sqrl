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

import java.util.Optional;
import org.apache.commons.lang3.ObjectUtils;

public class EnumUtil {

  public static <T extends Enum<T>> Optional<T> getByName(Class<T> enumClass, String name) {
    if (ObjectUtils.isEmpty(name)) {
      return Optional.empty();
    }
    try {
      return Optional.of(Enum.valueOf(enumClass, name.trim().toUpperCase()));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
