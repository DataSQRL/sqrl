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
package com.datasqrl.calcite.dialect;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeUtil;

public class ExtendedDuckDbSqlDialect extends ExtendedPostgresSqlDialect {

  public static final ExtendedPostgresSqlDialect DEFAULT =
      new ExtendedDuckDbSqlDialect(DEFAULT_CONTEXT);

  public ExtendedDuckDbSqlDialect(Context context) {
    super(context);
  }

  @Override
  public SqlDataTypeSpec getCastSpec(RelDataType type) {
    if (type instanceof MapSqlType) {
      return SqlTypeUtil.convertTypeToSpec(type);
    }

    return super.getCastSpec(type);
  }
}
