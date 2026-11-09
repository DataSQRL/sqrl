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

import graphql.schema.DataFetchingEnvironment;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Optional;
import lombok.Getter;

/**
 * Carries the bound statement of a streamed field out of graphql-java execution: the field's data
 * fetcher binds it here instead of running it, and the HTTP handler runs it through a cursor.
 */
@Getter
public final class PendingStream {

  private String sql;
  private Tuple params;

  public static Optional<PendingStream> of(DataFetchingEnvironment environment) {
    PendingStream pending = environment.getGraphQlContext().get(PendingStream.class);
    if (pending == null || environment.getExecutionStepInfo().getPath().getLevel() != 1) {
      return Optional.empty();
    }
    return Optional.of(pending);
  }

  public void bind(String sql, List<Object> params) {
    this.sql = sql;
    this.params = Tuple.from(params);
  }

  public boolean isBound() {
    return sql != null;
  }
}
