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

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JsonMergeUtils {

  public static void merge(ObjectNode target, ObjectNode update) {
    update
        .fields()
        .forEachRemaining(
            entry -> {
              var existing = target.get(entry.getKey());
              if (existing instanceof ObjectNode existingObject && entry.getValue().isObject()) {
                merge(existingObject, (ObjectNode) entry.getValue());
              } else {
                target.set(entry.getKey(), entry.getValue());
              }
            });
  }
}
