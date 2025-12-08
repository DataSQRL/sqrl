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
package com.datasqrl.planner;

import com.datasqrl.planner.analyzer.TableAnalysis;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

/**
 * This class primarily exists to collapse views that were expanded by Flink during planning so we
 * can create the DAG between the views. To collapse the views, we check if a relnode tree has been
 * seen before (i.e. is identical) and substitute the corresponding table in {@link
 * com.datasqrl.planner.analyzer.SQRLLogicalPlanAnalyzer#analyzeRelNode(RelNode)} In order to be
 * able to check for equality, we need to normalize the relnodes since planning introduces some
 * subtle differences.
 *
 * <p>Ideally, we would find a way to NOT expand the views in Flink so we can get rid of this
 * brittle "uncollapsing" code.
 */
@Value
public class TableAnalysisLookup {

  Map<ObjectIdentifier, TableAnalysis> id2SourceTable = new HashMap<>();
  Map<ObjectIdentifier, TableAnalysis> id2View = new HashMap<>();
  ListMultimap<Integer, TableAnalysis> viewMap = ArrayListMultimap.create();

  public TableAnalysis lookupSourceTable(@Nullable ObjectIdentifier objectId) {
    return id2SourceTable.get(objectId);
  }

  public Optional<TableAnalysis> lookupView(RelNode originalRelnode) {
    var hashCode = originalRelnode.getRowType().hashCode();
    if (viewMap.containsKey(hashCode)) {
      var normalizeRelnode = normalizeRelnode(originalRelnode);
      List<TableAnalysis> allMatches =
          viewMap.get(hashCode).stream()
              .filter(tbl -> matches(tbl, normalizeRelnode))
              .collect(Collectors.toList());
      // return last one in case there are multiple matches
      if (allMatches.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(allMatches.get(allMatches.size() - 1));
    } else {
      return Optional.empty();
    }
  }

  private static boolean matches(TableAnalysis tbl, RelNode otherRelNode) {
    if (tbl.getOriginalRelnode() == null) {
      return false;
    }
    return tbl.getOriginalRelnode().deepEquals(otherRelNode);
  }

  public TableAnalysis lookupView(ObjectIdentifier objectIdentifier) {
    return id2View.get(objectIdentifier);
  }

  public void removeView(ObjectIdentifier tableIdentifier) {
    var priorTable = id2View.remove(tableIdentifier);
    if (priorTable != null) {
      viewMap.remove(priorTable.getRowType().hashCode(), priorTable);
    }
  }

  public void registerTable(TableAnalysis tableAnalysis) {
    if (tableAnalysis.isSourceOrSink()) {
      id2SourceTable.put(tableAnalysis.getObjectIdentifier(), tableAnalysis);
    } else {
      viewMap.put(tableAnalysis.getRowType().hashCode(), tableAnalysis);
      id2View.put(tableAnalysis.getObjectIdentifier(), tableAnalysis);
    }
  }

  /**
   * Normalizes the Relnode so we can compare them (deeply) to determine equality of relational
   * trees: - resets Correlation variables. Those are incremented globally which means that
   * identical queries will have different correlation variable indexes.
   *
   * @param relNode
   * @return
   */
  public RelNode normalizeRelnode(RelNode relNode) {
    var correlIdNormalizer = new RelnodeNormalizer(relNode.getCluster().getRexBuilder());
    relNode = relNode.accept(correlIdNormalizer);
    return relNode;
  }

  /**
   * This class normalizes relnodes, so we can accurately compare them to determine when a table has
   * been expanded (and therefore should be collapsed in the DAG) The following causes differences
   * in Relnodes for identical (sub-)queries: - correlation ids: For correlation variables, the ids
   * are generated in Calcite using a global counter => we subtract the id of the first correlation
   * variable we encounter in the relnode tree from all ids we find - function names: during the
   * execution of an operation, Flink normalizes the function names but that doesn't happen during
   * planning => we upper case all names for BridgingSqlFunctions
   */
  private class RelnodeNormalizer extends RelShuttleImpl {

    RexBuilder rexBuilder;
    int correlationIdAdjustment = Integer.MIN_VALUE;

    public RelnodeNormalizer(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    private Optional<CorrelationId> adjustCorrelationId(CorrelationId correlationId) {
      if (correlationId.getName().startsWith(CorrelationId.CORREL_PREFIX)) {
        if (correlationIdAdjustment < 0) {
          correlationIdAdjustment = correlationId.getId() - 1;
        }
        if (correlationIdAdjustment == 0) {
          return Optional.empty();
        }
        return Optional.of(new CorrelationId(correlationId.getId() - correlationIdAdjustment));
      } else {
        return Optional.empty();
      }
    }

    private Optional<ContextResolvedFunction> normalizeFunction(
        ContextResolvedFunction resolvedFunc) {
      if (resolvedFunc.getIdentifier().isPresent()) {
        var funcId = resolvedFunc.getIdentifier().get();
        if (!funcId.getFunctionName().equals(funcId.getFunctionName().toUpperCase())) {
          FunctionIdentifier normalizedFuncId;
          if (funcId.getSimpleName().isPresent()) {
            normalizedFuncId = FunctionIdentifier.of(funcId.getSimpleName().get().toUpperCase());
          } else {
            var identifier = funcId.getIdentifier().get();
            var normalizedIdentifier =
                ObjectIdentifier.of(
                    identifier.getCatalogName(),
                    identifier.getDatabaseName(),
                    identifier.getObjectName().toUpperCase());
            normalizedFuncId = FunctionIdentifier.of(normalizedIdentifier);
          }
          var normalizedResolvedFunc =
              ContextResolvedFunction.temporary(normalizedFuncId, resolvedFunc.getDefinition());
          return Optional.of(normalizedResolvedFunc);
        }
      }
      return Optional.empty();
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      List<AggregateCall> updatedCalls =
          aggregate.getAggCallList().stream()
              .map(
                  aggCall -> {
                    var aggFct = aggCall.getAggregation();
                    if (aggFct instanceof BridgingSqlAggFunction func) {
                      var normalizedFct = normalizeFunction(func.getResolvedFunction());
                      if (normalizedFct.isPresent()) {
                        var normalizedFunc =
                            BridgingSqlAggFunction.of(
                                func.getDataTypeFactory(),
                                func.getTypeFactory(),
                                func.getKind(),
                                normalizedFct.get(),
                                func.getTypeInference());
                        return AggregateCall.create(
                            normalizedFunc,
                            aggCall.isDistinct(),
                            aggCall.isApproximate(),
                            aggCall.ignoreNulls(),
                            aggCall.rexList,
                            aggCall.getArgList(),
                            aggCall.filterArg,
                            aggCall.distinctKeys,
                            aggCall.getCollation(),
                            aggCall.getType(),
                            aggCall.getName());
                      }
                    }
                    return aggCall;
                  })
              .collect(Collectors.toList());
      aggregate =
          aggregate.copy(
              aggregate.getTraitSet(),
              aggregate.getInput(),
              aggregate.getGroupSet(),
              aggregate.getGroupSets(),
              updatedCalls);
      return super.visit(aggregate);
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
      var adjustedId = adjustCorrelationId(correlate.getCorrelationId());
      if (adjustedId.isPresent()) {
        var left = correlate.getLeft().accept(this);
        var right = correlate.getRight().accept(this);
        return correlate.copy(
            correlate.getTraitSet(),
            left,
            right,
            adjustedId.get(),
            correlate.getRequiredColumns(),
            correlate.getJoinType());
      } else {
        return super.visit(correlate);
      }
    }

    @Override
    public RelNode visit(RelNode relNode) {
      if (relNode instanceof LogicalTableFunctionScan && relNode.getInputs().isEmpty()) {
        // Since we only visit the children with the rexshuttle below,
        // we have to explicitly visit the table function scan since it is a sink (i.e. no children)
        // but can have RexNodes (a TableScan cannot)
        return relNode.accept(rexNormalizer);
      } else {
        return super.visit(relNode);
      }
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      if (i == 0) {
        parent = parent.accept(rexNormalizer);
      }
      return super.visitChild(parent, i, child);
    }

    RexShuttle rexNormalizer = new RexNormalizer();

    class RexNormalizer extends RexShuttle {

      @Override
      public RexNode visitCorrelVariable(RexCorrelVariable variable) {
        var adjustedId = adjustCorrelationId(variable.id);
        if (adjustedId.isPresent()) {
          return rexBuilder.makeCorrel(variable.getType(), adjustedId.get());
        } else {
          return super.visitCorrelVariable(variable);
        }
      }

      @Override
      public RexNode visitSubQuery(RexSubQuery subQuery) {
        var rewritten = subQuery.rel.accept(RelnodeNormalizer.this);
        return subQuery.clone(rewritten);
      }

      @Override
      public RexNode visitCall(RexCall call) {
        call = (RexCall) super.visitCall(call);
        if (call.getOperator() instanceof BridgingSqlFunction) {
          var func = (BridgingSqlFunction) call.getOperator();
          var normalizedFct = normalizeFunction(func.getResolvedFunction());
          if (normalizedFct.isPresent()) {
            var normalizedFunc =
                BridgingSqlFunction.of(
                    func.getDataTypeFactory(),
                    func.getTypeFactory(),
                    func.getRexFactory(),
                    func.kind,
                    normalizedFct.get(),
                    func.getTypeInference());
            return rexBuilder.makeCall(normalizedFunc, call.getOperands());
          }
        }
        return call;
      }
    }
  }
}
