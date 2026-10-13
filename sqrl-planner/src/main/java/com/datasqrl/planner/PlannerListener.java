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
package com.datasqrl.planner;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.ParsedStatement;
import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.tables.SqrlTableFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * Receives what the {@link SqlScriptPlanner} learns while it plans a script, for tools such as a
 * language server that need to know where tables are defined and what the names in queries refer
 * to. All methods do nothing by default.
 */
public interface PlannerListener {

  PlannerListener NONE = new PlannerListener() {};

  /**
   * Called before a statement is planned. The statements of a script that a statement imports are
   * planned before that statement finishes, so calls can be nested. Throwing a {@link
   * java.util.concurrent.CancellationException} stops planning, also in an imported script.
   *
   * @param statement the statement
   * @param location the location of the statement in its script
   */
  default void statementStarted(ParsedStatement statement, ErrorLocation location) {}

  /** Called after the statement that started last has been planned, also if that failed. */
  default void statementFinished() {}

  /**
   * Called when a table is added.
   *
   * @param table the planned table
   * @param location where the table is defined, which is in another script for imported tables
   */
  default void tableDefined(TableAnalysis table, ErrorLocation location) {}

  /**
   * Called when a table function or relationship is added.
   *
   * @param function the planned function
   * @param location where the function is defined
   */
  default void functionDefined(SqrlTableFunction function, ErrorLocation location) {}

  /**
   * Called when the current statement refers to a table outside of a query: the table of an EXPORT,
   * the target of an INSERT, or the table that a relationship or column is added to.
   *
   * @param table the table
   * @param name the name of the table as written, with its location relative to the statement
   */
  default void tableReferenced(ObjectIdentifier table, ParsedObject<NamePath> name) {}

  /**
   * Called when a query of the current statement has been validated, so that the names in it can be
   * resolved with the validator, e.g. with {@link QueryReferences}. Positions in the query are
   * relative to the SQL of the statement, see {@link SQLStatement#mapSqlLocation}.
   *
   * @param query the validated query
   * @param validator the validator that validated it
   */
  default void queryValidated(SqlNode query, SqlValidator validator) {}
}
