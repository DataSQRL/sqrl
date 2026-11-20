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
package com.datasqrl.planner.contract;

import java.util.Optional;

/**
 * Finds the data contract of the tables that one file defines. The contract of a table is the
 * schema object of the table's name, ignoring case, in the contract file that sits next to the
 * file.
 *
 * <p>A table that takes over the columns of a schema table with {@code LIKE}, directly or through
 * other tables, also finds its schema object in the contract file next to the file that defines the
 * schema table. That way a table that is defined once per environment has one contract, next to the
 * file with the columns that the environments share.
 */
public interface ContractSource {

  /** The source of tables whose defining file is unknown, which have no contract */
  ContractSource NONE = (tableName, isSchemaTable, extended) -> TableContract.none(tableName);

  /**
   * @param tableName the name of a table that the file of this source defines, as its {@code CREATE
   *     TABLE} statement writes it
   * @param isSchemaTable whether the table only declares columns for other tables
   * @param extended the contract of the table whose columns the table takes over with {@code LIKE},
   *     if it has such a clause
   * @return the contract of the table
   */
  TableContract forTable(String tableName, boolean isSchemaTable, Optional<TableContract> extended);

  /** The contract of a table that declares all its columns itself. */
  default TableContract forTable(String tableName) {
    return forTable(tableName, false, Optional.empty());
  }
}
