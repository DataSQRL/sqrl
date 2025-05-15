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
package com.datasqrl.module;

import com.datasqrl.canonicalizer.Name;
import org.apache.calcite.schema.Table;

/** Represents a {@link NamespaceObject} for a table. */
public interface TableNamespaceObject<T> extends NamespaceObject {

  /**
   * Returns the name of the table.
   *
   * @return the name of the table
   */
  @Override
  Name getName();

  /**
   * Returns the {@link Table} of the table.
   *
   * @return the {@link Table} of the table
   */
  T getTable();
}
