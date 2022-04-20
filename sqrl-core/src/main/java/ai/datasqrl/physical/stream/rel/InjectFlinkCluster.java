package ai.datasqrl.physical.stream.rel;

import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableSet;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.trait.MiniBatchIntervalTrait;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTrait;
import org.apache.flink.table.planner.plan.trait.UpdateKindTrait;

/**
 * The plan is created with a SQRL calcite cluster which needs to be migrated to the flink
 * calcite cluster
 */
@AllArgsConstructor
public class InjectFlinkCluster extends RelShuttleImpl {
  TableEnvironmentImpl tEnv;
  RelOptCluster cluster;
  RelTraitSet defaultTrait;

  public static RelNode injectFlinkRelOptCluster(StreamTableEnvironmentImpl tEnv, RelNode input) {
    RelOptCluster cluster = RelOptClusterExtractor.extract(tEnv.listTables()[0], tEnv);
    return injectFlinkRelOptCluster(tEnv, input, cluster);
  }

  private static RelNode injectFlinkRelOptCluster(StreamTableEnvironmentImpl tEnv, RelNode relNode, RelOptCluster cluster) {
    RelTraitSet defaultTraits = RelTraitSet.createEmpty().plus(Convention.NONE)
        .plus(FlinkRelDistribution.ANY())
        .plus(MiniBatchIntervalTrait.NONE())
        .plus(ModifyKindSetTrait.EMPTY())
        .plus(UpdateKindTrait.NONE());

    return relNode.accept(new InjectFlinkCluster(tEnv, cluster, defaultTraits));
  }

  @Override
  public RelNode visit(TableScan scan) {
    //Lookup TableSourceTable in flink, add statistics over
    PlannerQueryOperation op = (PlannerQueryOperation) tEnv.getParser().parse("select * from "
        + scan.getTable().getQualifiedName().get(scan.getTable().getQualifiedName().size()-1)).get(0);
    return op.getCalciteTree().getInput(0);
  }

  @Override
  public RelNode visit(TableFunctionScan scan) {
    List<RelNode> inputs = scan.getInputs().stream()
        .map(this::visit)
        .collect(Collectors.toList());
    return new LogicalTableFunctionScan(cluster, defaultTrait, inputs, scan.getCall(),
        scan.getElementType(), scan.getRowType(), scan.getColumnMappings());
  }

  @Override
  public RelNode visit(LogicalValues values) {
    return new LogicalValues(cluster, defaultTrait, values.getRowType(), values.tuples);
  }

  @Override
  public RelNode visit(LogicalCorrelate correlate) {
    return new LogicalCorrelate(cluster,
        defaultTrait, correlate.getLeft().accept(this), correlate.getRight().accept(this),
        correlate.getCorrelationId(), correlate.getRequiredColumns(), correlate.getJoinType());
  }

  @Override
  public RelNode visit(LogicalProject project) {
    return new LogicalProject(cluster, defaultTrait, project.getHints(), project.getInput().accept(this),
        project.getProjects(), project.getRowType());
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    return new LogicalAggregate(cluster, defaultTrait, aggregate.getHints(),
        aggregate.getInput().accept(this),
        aggregate.getGroupSet(),
        aggregate.getGroupSets(), aggregate.getAggCallList());
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    return new LogicalFilter(cluster, defaultTrait, filter.getInput().accept(this), filter.getCondition(),
        (ImmutableSet<CorrelationId>) filter.getVariablesSet());
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    return new LogicalJoin(cluster,
        defaultTrait, join.getHints(), join.getLeft().accept(this), join.getRight().accept(this),
        join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
        (ImmutableList<RelDataTypeField>) join.getSystemFieldList());
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    return super.visit(union);
  }

  @Override
  public RelNode visit(LogicalIntersect intersect) {
    return super.visit(intersect);
  }

  @Override
  public RelNode visit(LogicalMinus minus) {
    return super.visit(minus);
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    throw new RuntimeException("sort todo");
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof Uncollect) {
      Uncollect uncollect = (Uncollect) other;
      return new Uncollect(cluster, defaultTrait, uncollect.getInput().accept(this),
          uncollect.withOrdinality, List.of());
    } else if (other instanceof LogicalWatermarkAssigner) {
      LogicalWatermarkAssigner watermarkAssigner = (LogicalWatermarkAssigner)other;
      return new LogicalWatermarkAssigner(cluster, defaultTrait, watermarkAssigner.getInput().accept(this),
          watermarkAssigner.rowtimeFieldIndex(), watermarkAssigner.watermarkExpr());
    }
    throw new RuntimeException("not yet implemented:" + other.getClass());
  }
}
