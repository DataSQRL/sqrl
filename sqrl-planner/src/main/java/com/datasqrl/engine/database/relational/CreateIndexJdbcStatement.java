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
package com.datasqrl.engine.database.relational;

import com.datasqrl.deployment.model.JdbcStatementModel.Field;
import com.datasqrl.deployment.model.JdbcStatementModel.Type;
import com.datasqrl.plan.global.IndexDefinition;
import java.util.List;
import lombok.Value;

/**
 * A CREATE INDEX {@link JdbcStatement} which keeps the {@link IndexDefinition} it was created from,
 * so the selected indexes can be reported per table.
 */
@Value
public class CreateIndexJdbcStatement implements JdbcStatement {

  /** The index this statement creates */
  IndexDefinition index;

  /** The SQL representation of this statement */
  String sql;

  @Override
  public String getName() {
    return index.getName();
  }

  @Override
  public Type getType() {
    return Type.INDEX;
  }

  @Override
  public String getDescription() {
    return null;
  }

  @Override
  public List<Field> getFields() {
    return null;
  }
}
