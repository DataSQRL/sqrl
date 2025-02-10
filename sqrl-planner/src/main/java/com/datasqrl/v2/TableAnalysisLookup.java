package com.datasqrl.v2;

import com.datasqrl.calcite.function.OperatorRuleTransform;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.flink.table.catalog.ObjectIdentifier;

@Value
public class TableAnalysisLookup {

  Map<ObjectIdentifier, TableAnalysis> sourceTableMap = new HashMap<>();
  HashMultimap<Integer, TableAnalysis> tableMap = HashMultimap.create();
  Map<ObjectIdentifier, TableAnalysis> id2Table = new HashMap<>();

  public TableAnalysis lookupSourceTable(ObjectIdentifier objectId) {
    return sourceTableMap.get(objectId);
  }

  public Optional<TableAnalysis> lookupTable(RelNode originalRelnode) {
    int hashCode = originalRelnode.getRowType().hashCode();
    if (tableMap.containsKey(hashCode)) {
      RelNode normalizeRelnode = normalizeRelnode(originalRelnode);
      return tableMap.get(hashCode).stream().filter(tbl -> matches(tbl, normalizeRelnode)).findFirst();
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
    CorrelationIdNormalizer correlIdNormalizer = new CorrelationIdNormalizer(relNode.getCluster().getRexBuilder());
    relNode = relNode.accept(correlIdNormalizer);
    return relNode;
  }

  private class CorrelationIdNormalizer extends RelShuttleImpl {

    RexBuilder rexBuilder;
    int correlationIdAdjustment = Integer.MIN_VALUE;

    public CorrelationIdNormalizer(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    public boolean hasAdjusted() {
      return correlationIdAdjustment >= 0;
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
        return relNode.accept(correlVariableNormalizer);
      } else {
        return super.visit(relNode);
      }
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      if (i==0 && hasAdjusted()) parent = parent.accept(correlVariableNormalizer);
      return super.visitChild(parent, i, child);
    }

    RexShuttle correlVariableNormalizer = new RexShuttle() {

      @Override
      public RexNode visitCorrelVariable(RexCorrelVariable variable) {
        Optional<CorrelationId> adjustedId = adjustCorrelationId(variable.id);
        if (adjustedId.isPresent()) {
          return rexBuilder.makeCorrel(variable.getType(), adjustedId.get());
        } else {
          return super.visitCorrelVariable(variable);
        }
      }
    };

  }



}
