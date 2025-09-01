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
package com.datasqrl.calcite;

import com.datasqrl.calcite.function.OperatorRuleTransform;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.Programs;

/**
 * Converts functions to the given dialect via rule transformations. Rule transformations are
 * implemented as {@link OperatorRuleTransform} for "structural" transformations that have to happen
 * at the {@link RelNode} level.
 *
 * <p>Simpler transformations that only require switching out the function name or parameter order
 * should be implemented via {@link com.datasqrl.function.translations.SqlTranslation} which happen
 * during unparsing.
 */
public class OperatorRuleTransformer {

  private final Dialect dialect;
  private final Map<Name, OperatorRuleTransform> transformMap;

  public OperatorRuleTransformer(Dialect dialect) {
    this.dialect = dialect;
    // Lookup all transformations for this dialect by service discovery
    this.transformMap =
        ServiceLoaderDiscovery.getAll(OperatorRuleTransform.class).stream()
            .filter(transform -> transform.getDialect() == dialect)
            .collect(Collectors.toMap(t -> Name.lower(t.getRuleOperatorName()), t -> t));
  }

  public RelNode convert(RelNode relNode) {
    // Identify all functions that require transformations and add them to the rule set
    var transforms = extractFunctionTransforms(relNode);
    List<RelRule> rules =
        transforms.entrySet().stream()
            .flatMap(transform -> transform.getValue().transform(transform.getKey()).stream())
            .collect(Collectors.toList());
    // Apply the rules to relnode
    relNode =
        Programs.hep(rules, false, null)
            .run(null, relNode, relNode.getTraitSet(), List.of(), List.of());

    return relNode;
  }

  private Map<SqlOperator, OperatorRuleTransform> extractFunctionTransforms(RelNode relNode) {
    Map<SqlOperator, OperatorRuleTransform> transforms = new HashMap<>();
    relNode.accept(
        new RelShuttleImpl() {

          @Override
          public RelNode visit(LogicalAggregate aggregate) {
            for (AggregateCall call : aggregate.getAggCallList()) {
              var transform = transformMap.get(Name.lower(call.getAggregation().getName()));
              if (transform != null) {
                transforms.put(call.getAggregation(), transform);
              }
            }
            return super.visit(aggregate);
          }

          @Override
          protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            parent.accept(
                new RexShuttle() {
                  @Override
                  public RexNode visitCall(RexCall call) {
                    var transform = transformMap.get(Name.lower(call.getOperator().getName()));
                    if (transform != null) {
                      transforms.put(call.getOperator(), transform);
                    }
                    return super.visitCall(call);
                  }
                });

            return super.visitChild(parent, i, child);
          }
        });

    return new HashMap<>(transforms);
  }
}
