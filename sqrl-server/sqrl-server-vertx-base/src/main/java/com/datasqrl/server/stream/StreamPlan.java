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
package com.datasqrl.server.stream;

import graphql.ExceptionWhileDataFetching;
import graphql.ExecutionResult;
import graphql.GraphQLError;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nullable;

/** What an eligible request streams: one top-level list field and its scalar columns. */
public record StreamPlan(
    String responseKey,
    boolean nonNullField,
    @Nullable SourceLocation location,
    List<StreamedColumn> columns) {

  GraphQLError error(Throwable cause) {
    return new ExceptionWhileDataFetching(
        ResultPath.rootPath().segment(responseKey), cause, location);
  }

  /** The response the non-streaming path sends when the field's query fails. */
  Buffer errorResponse(Throwable cause) {
    var data = new HashMap<String, Object>();
    data.put(responseKey, null);
    var result =
        ExecutionResult.newExecutionResult()
            .data(nonNullField ? null : data)
            .addError(error(cause))
            .build();
    return new JsonObject(result.toSpecification()).toBuffer();
  }
}
