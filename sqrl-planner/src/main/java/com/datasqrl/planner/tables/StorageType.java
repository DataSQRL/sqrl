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
package com.datasqrl.planner.tables;

import java.util.Locale;

/** The kind of object that stores the data of a table. */
public enum StorageType {
  TOPIC,
  TABLE,
  VIEW,
  FILE;

  /** The kind as the compiler writes it for other systems, e.g. {@code topic}. */
  public String getDisplayName() {
    return name().toLowerCase(Locale.ROOT);
  }
}
