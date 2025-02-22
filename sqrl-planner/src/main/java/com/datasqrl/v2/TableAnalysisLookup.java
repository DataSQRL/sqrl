package com.datasqrl.v2;

import com.datasqrl.v2.analyzer.TableAnalysis;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

@Value
public class TableAnalysisLookup {

  Map<ObjectIdentifier, TableAnalysis> sourceTableMap = new HashMap<>();
  ListMultimap<Integer, TableAnalysis> tableMap = ArrayListMultimap.create();
  Map<ObjectIdentifier, TableAnalysis> id2Table = new HashMap<>();

  public TableAnalysis lookupSourceTable(ObjectIdentifier objectId) {
    return sourceTableMap.get(objectId);
  }

  public Optional<TableAnalysis> lookupTable(RelNode originalRelnode) {
    int hashCode = originalRelnode.getRowType().hashCode();
    if (tableMap.containsKey(hashCode)) {
      RelNode normalizeRelnode = normalizeRelnode(originalRelnode);
      List<TableAnalysis> allMatches = tableMap.get(hashCode).stream().filter(tbl -> matches(tbl, normalizeRelnode)).collect(
          Collectors.toList());
      //return last one in case there are multiple matches
      if (allMatches.isEmpty()) return Optional.empty();
      return Optional.of(allMatches.get(allMatches.size()-1));
    } else return Optional.empty();
  }

  private static boolean matches(TableAnalysis tbl, RelNode otherRelNode) {
    if (tbl.getOriginalRelnode()==null) return false;
    return tbl.getOriginalRelnode().deepEquals(otherRelNode);
  }

  public TableAnalysis lookupTable(ObjectIdentifier objectIdentifier) {
    return id2Table.get(objectIdentifier);
  }

  public void removeTable(ObjectIdentifier tableIdentifier) {
    TableAnalysis priorTable = id2Table.remove(tableIdentifier);
    if (priorTable != null && !priorTable.isSourceOrSink()) {
      tableMap.remove(priorTable.getRowType().hashCode(), priorTable);
    }
  }

  public void registerTable(TableAnalysis tableAnalysis) {
    if (tableAnalysis.isSourceOrSink()) {
      sourceTableMap.put(tableAnalysis.getIdentifier(), tableAnalysis);
    } else {
      tableMap.put(tableAnalysis.getRowType().hashCode(), tableAnalysis);
    }
    id2Table.put(tableAnalysis.getIdentifier(), tableAnalysis);
  }

  /**
   * Normalizes the Relnode so we can compare them (deeply) to determine equality of relational trees:
   * - resets Correlation variables. Those are incremented globally which means that identical queries
   * will have different correlation variable indexes.
   * @param relNode
   * @return
   */
  public RelNode normalizeRelnode(RelNode relNode) {
    RelnodeNormalizer correlIdNormalizer = new RelnodeNormalizer(relNode.getCluster().getRexBuilder());
    relNode = relNode.accept(correlIdNormalizer);
    return relNode;
  }

  /**
   * This class normalizes relnodes, so we can accurately compare them to determine
   * when a table has been expanded (and therefore should be collapsed in the DAG)
   * The following causes differences in Relnodes for identical (sub-)queries:
   * - correlation ids: For correlation variables, the ids are generated in Calcite using a global counter
   *    => we subtract the id of the first correlation variable we encounter in the relnode tree from all ids we find
   * - function names: during the execution of an operation, Flink normalizes the function names but that doesn't happen during planning
   *    => we upper case all names for BridgingSqlFunctions
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
          correlationIdAdjustment = correlationId.getId()-1;
        }
        if (correlationIdAdjustment == 0) return Optional.empty();
        return Optional.of(new CorrelationId(correlationId.getId()-correlationIdAdjustment));
      } else return Optional.empty();
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      List<AggregateCall> aggCalls = aggregate.getAggCallList();
      for (AggregateCall aggCall : aggCalls) {
      }
      return super.visit(aggregate);
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
      Optional<CorrelationId> adjustedId = adjustCorrelationId(correlate.getCorrelationId());
      if (adjustedId.isPresent()) {
        RelNode left = correlate.getLeft().accept(this);
        RelNode right = correlate.getRight().accept(this);
        return correlate.copy(correlate.getTraitSet(), left, right, adjustedId.get(), correlate.getRequiredColumns(), correlate.getJoinType());
      } else {
        return super.visit(correlate);
      }
    }

    @Override
    public RelNode visit(RelNode relNode) {
      if (relNode instanceof LogicalTableFunctionScan && relNode.getInputs().isEmpty()) {
        //Since we only visit the children with the rexshuttle below,
        //we have to explicitly visit the table function scan since it is a sink (i.e. no children)
        //but can have RexNodes (a TableScan cannot)
        return relNode.accept(rexNormalizer);
      } else {
        return super.visit(relNode);
      }
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      if (i==0) parent = parent.accept(rexNormalizer);
      return super.visitChild(parent, i, child);
    }

    RexShuttle rexNormalizer = new RexNormalizer();

    class RexNormalizer extends RexShuttle {

      @Override
      public RexNode visitCorrelVariable(RexCorrelVariable variable) {
        Optional<CorrelationId> adjustedId = adjustCorrelationId(variable.id);
        if (adjustedId.isPresent()) {
          return rexBuilder.makeCorrel(variable.getType(), adjustedId.get());
        } else {
          return super.visitCorrelVariable(variable);
        }
      }

      @Override
      public RexNode visitSubQuery(RexSubQuery subQuery) {
        RelNode rewritten = subQuery.rel.accept(RelnodeNormalizer.this);
        return subQuery.clone(rewritten);
      }

      @Override
      public RexNode visitCall(RexCall call) {
        call = (RexCall) super.visitCall(call);
        if (call.getOperator() instanceof BridgingSqlFunction) {
          BridgingSqlFunction func = (BridgingSqlFunction) call.getOperator();
          ContextResolvedFunction resolvedFunc = func.getResolvedFunction();
          if (resolvedFunc.getIdentifier().isPresent()) {
            FunctionIdentifier funcId = resolvedFunc.getIdentifier().get();
            if (!funcId.getFunctionName().equals(funcId.getFunctionName().toUpperCase())) {
              FunctionIdentifier normalizedFuncId;
              if (funcId.getSimpleName().isPresent()) {
                normalizedFuncId = FunctionIdentifier.of(
                    funcId.getSimpleName().get().toUpperCase());
              } else {
                ObjectIdentifier identifier = funcId.getIdentifier().get();
                ObjectIdentifier normalizedIdentifier = ObjectIdentifier.of(
                    identifier.getCatalogName(), identifier.getDatabaseName(),
                    identifier.getObjectName().toUpperCase());
                normalizedFuncId = FunctionIdentifier.of(normalizedIdentifier);
              }
              ContextResolvedFunction normalizedResolvedFunc = ContextResolvedFunction.temporary(
                  normalizedFuncId,
                  resolvedFunc.getDefinition());
              BridgingSqlFunction normalizedFunc = BridgingSqlFunction.of(func.getDataTypeFactory(),
                  func.getTypeFactory(),
                  func.getRexFactory(), func.kind, normalizedResolvedFunc, func.getTypeInference());
              return rexBuilder.makeCall(normalizedFunc, call.getOperands());
            }
          }
        }
        return call;
      }
    }

  }



}
