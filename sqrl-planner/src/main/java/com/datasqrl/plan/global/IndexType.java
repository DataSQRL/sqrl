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
package com.datasqrl.plan.global;

import java.util.Optional;

public enum IndexType {
  HASH,
  BTREE,
  PBTREE,
  TEXT,
  VECTOR_COSINE,
  VECTOR_EUCLID;

  public boolean requiresAllColumns() {
    return this == HASH;
  }

  /**
   * A general index covers comparison operators and can cover multiple columns. If it is not a
   * general index, it is a function index that has a specific indexing method.
   *
   * @return
   */
  public boolean isGeneralIndex() {
    return this == HASH || this == BTREE || this == PBTREE;
  }

  public boolean isPartitioned() {
    return this == PBTREE;
  }

  public static Optional<IndexType> fromName(String name) {
    for (IndexType indexType : IndexType.values()) {
      if (indexType.name().equalsIgnoreCase(name)) {
        return Optional.of(indexType);
      }
    }
    return Optional.empty();
  }
}
