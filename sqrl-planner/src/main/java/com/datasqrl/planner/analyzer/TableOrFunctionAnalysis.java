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
package com.datasqrl.planner.analyzer;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.tables.TableType;
import org.apache.flink.table.catalog.ObjectIdentifier;

public interface TableOrFunctionAnalysis extends AbstractAnalysis {

  /**
   * A unique identifier for tables and functions defined in a SQRL script. This is used in the DAG
   * to uniquely identify the nodes.
   *
   * <p>We need to distinguish between tables that define a connection to a source or sink (i.e.
   * CREATE TABLE in Flink language) and those that defined intermediate tables (i.e. "views" in
   * Flink language) by adding the sourceOrSink boolean flag to identify the former since we create
   * views for sources with the same name implicitly.
   *
   * @param objectIdentifier
   * @param sourceOrSink
   */
  record UniqueIdentifier(ObjectIdentifier objectIdentifier, boolean sourceOrSink) {

    public boolean isHidden() {
      return Name.isHiddenString(objectIdentifier.getObjectName());
    }

    /**
     * Returns the name of this unique identifier. For sourceOrSink we append a suffix since those
     * are connector tables.
     *
     * @return
     */
    @Override
    public String toString() {
      if (sourceOrSink) {
        return objectIdentifier.asSummaryString() + "__base";
      }
      return objectIdentifier.asSummaryString();
    }
  }

  UniqueIdentifier getIdentifier();

  public TableType getType();

  public boolean isSourceOrSink();

  /**
   * The base table on which this function is defined. This means, that this table or function
   * returns the same type as the base table.
   */
  TableAnalysis getBaseTable();
}
