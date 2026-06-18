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
package com.datasqrl.server.jdbc;

import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import com.datasqrl.server.util.SqlTypeConverter;
import io.vertx.core.json.JsonArray;
import io.vertx.sqlclient.data.NullValue;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class VertxParamArgumentTypeMapper implements ParamArgumentTypeMapper {

  @Override
  public Object map(Object param, Optional<String> sqlType) {
    if (param == null && sqlType.isPresent()) {
      var cls = SqlTypeConverter.sqlTypeNameToJavaClass(sqlType.get());
      return NullValue.of(cls);
    }

    if (param instanceof List<?> l) {
      return l.toArray();
    }

    if (param instanceof JsonArray arr) {
      // Unwrap JsonArray to plain Java array to avoid pgclient treating it as JSONB
      return arr.getList().toArray();
    }

    if (param instanceof FlinkVectorType vec) {
      return Arrays.toString(vec.getValue());
    }

    return param;
  }
}
