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
package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.RelToSqlNode;
import com.datasqrl.engine.stream.flink.sql.model.QueryPipelineItem;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter;
import org.apache.calcite.sql.SqlNode;

@AutoService(RelToSqlNode.class)
public class FlinkRelToSqlNode implements RelToSqlNode {

  @Getter AtomicInteger atomicInteger = new AtomicInteger();

  @Override
  public FlinkSqlNodes convert(RelNode relNode) {
    var relToSqlConverter = new FlinkRelToSqlConverter(atomicInteger);
    var sqlNode = relToSqlConverter.visitRoot(relNode).asStatement();

    return new FlinkSqlNodes(sqlNode, relToSqlConverter.getQueries());
  }

  @Override
  public Dialect getDialect() {
    return Dialect.FLINK;
  }

  @Value
  public static class FlinkSqlNodes implements SqlNodes {

    SqlNode sqlNode;
    List<QueryPipelineItem> queryList;
  }
}
