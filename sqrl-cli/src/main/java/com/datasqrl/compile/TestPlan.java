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
package com.datasqrl.compile;

import com.datasqrl.engine.database.relational.JdbcStatement;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class TestPlan {

  List<JdbcStatement> jdbcViews;
  List<GraphqlQuery> queries;
  List<GraphqlQuery> mutations;
  List<GraphqlQuery> subscriptions;

  @AllArgsConstructor
  @NoArgsConstructor
  @Getter
  public static class GraphqlQuery {
    String version;
    String name;
    String query;
    Map<String, String> headers;
  }
}
