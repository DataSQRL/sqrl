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

import com.datasqrl.io.tables.TableType;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.table.catalog.ObjectIdentifier;

public interface TableOrFunctionAnalysis extends AbstractAnalysis {

  ObjectIdentifier getIdentifier();

  List<String> getParameterNames();

  public TableType getType();

  default FullIdentifier getFullIdentifier() {
    return new FullIdentifier(getIdentifier(), getParameterNames());
  }

  /**
   * The base table on which this function is defined. This means, that this table or function
   * returns the same type as the base table.
   */
  TableAnalysis getBaseTable();

  /**
   * A full identifier combines the object identifier with the parameter list to unqiuely quality a
   * table or function within the catalog.
   *
   * <p>Note, functions are not uniquely qualified by object identifier alone since overloaded
   * functions have the same identifier but a different argument signature.
   */
  @Value
  @AllArgsConstructor
  class FullIdentifier {
    ObjectIdentifier objectIdentifier;
    List<String> arguments;

    public FullIdentifier(ObjectIdentifier objectIdentifier) {
      this(objectIdentifier, Collections.EMPTY_LIST);
    }
  }
}
