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
package com.datasqrl.function.vector;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import com.datasqrl.flinkrunner.stdlib.vector.VectorFunctions;
import com.datasqrl.sql.DatabaseExtension;
import com.datasqrl.util.FunctionUtil;
import com.google.auto.service.AutoService;
import java.util.Set;
import java.util.stream.Collectors;

@AutoService(DatabaseExtension.class)
public class VectorPgExtension implements DatabaseExtension {
  public static final String ddlStatement = "CREATE EXTENSION IF NOT EXISTS vector";

  @Override
  public Class typeClass() {
    return FlinkVectorType.class;
  }

  @Override
  public Set<Name> operators() {
    return VectorFunctions.functions.stream()
        .map(f -> FunctionUtil.getFunctionName(f.getClass()))
        .collect(Collectors.toSet());
  }

  @Override
  public String getExtensionDdl() {
    return ddlStatement;
  }
}
