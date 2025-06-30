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
package com.datasqrl.graphql.server.query;

import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Getter
@NoArgsConstructor
public class ResolvedSqlQuery implements ResolvedQuery {

  SqlQuery query;
  PreparedQueryContainer preparedQueryContainer;
  Optional<Preprocessor> preprocessor;

  @Override
  public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context) {
    return visitor.visitResolvedSqlQuery(this, context);
  }

  @Override
  public ResolvedSqlQuery preprocess(QueryExecutionContext context) {
    return preprocessor.map(pre -> pre.preprocess(this, context)).orElse(this);
  }
}
