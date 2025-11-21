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
package com.datasqrl.graphql.util;

import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.PropertyDataFetcher;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.function.Supplier;

public class CaseInsensitiveJsonDataFetcher extends PropertyDataFetcher<Object> {

  public CaseInsensitiveJsonDataFetcher(String propertyName) {
    super(propertyName);
  }

  @Override
  public Object get(DataFetchingEnvironment environment) {
    var source = environment.getSource();
    if (source instanceof JsonObject jsonObj) {
      return fetchJsonObject(jsonObj);
    }

    return super.get(environment);
  }

  @Override
  public Object get(
      GraphQLFieldDefinition fieldDef, Object source, Supplier<DataFetchingEnvironment> envSupplier)
      throws Exception {

    if (source instanceof JsonObject jsonObj) {
      return fetchJsonObject(jsonObj);
    }

    return super.get(fieldDef, source, envSupplier);
  }

  Object fetchJsonObject(JsonObject jsonObj) {
    var value = jsonObj.getValue(getPropertyName());
    if (value != null) {
      return value;
    }
    // Case-insensitive lookup for drivers that may not preserve sensitivity
    return jsonObj.getMap().entrySet().stream()
        .filter(e -> e.getKey().equalsIgnoreCase(getPropertyName()))
        .filter(e -> e.getValue() != null)
        .map(Map.Entry::getValue)
        .findAny()
        .orElse(null);
  }
}
