/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql2rel;

import static com.datasqrl.util.ReflectionUtil.invokeSuperPrivateMethod;

import com.datasqrl.plan.hints.JoinModifierHint;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.sql.JoinModifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;

@SuppressWarnings("UnstableApiUsage")
public class SqrlSqlToRelConverter extends SqlToRelConverter {

  private final RelBuilder relBuilder;

  public SqrlSqlToRelConverter(ViewExpander viewExpander, SqlValidator validator,
      CatalogReader catalogReader, RelOptCluster cluster,
      SqlRexConvertletTable convertletTable, Config config) {
    super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    this.relBuilder =
        config.getRelBuilderFactory()
            .create(cluster, null)
            .transform(config.getRelBuilderConfigTransform());
  }

  protected void convertFrom(Blackboard bb, SqlNode from, List<String> fieldNames) {
    if (from == null) {
      bb.setRoot(LogicalValues.createOneRow(cluster), false);
      return;
    }

    switch (from.getKind()) {
      case JOIN:
        SqlJoin from1 = (SqlJoin) from;
        invokeSuperPrivateMethod(this, "convertJoin",
              List.of(Blackboard.class, SqlJoin.class), bb, from1);

        // Sqrl: Add hint
        if (from1.getModifier() != null) {
          RelHint hint = new JoinModifierHint(JoinModifier.valueOf(from1.getModifier().toValue()))
              .getHint();
          RelNode joinRel = relBuilder.push(bb.root)
              .hints(hint)
              .build();
          RelOptUtil.propagateRelHints(joinRel, false);
          bb.setRoot(joinRel, false);
        }
        return;
    }

    super.convertFrom(bb, from, fieldNames);
  }
}
