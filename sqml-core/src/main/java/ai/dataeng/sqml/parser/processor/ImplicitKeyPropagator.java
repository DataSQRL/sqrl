package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.parser.translator.RelColumnTranslator;
import ai.dataeng.sqml.planner.optimize2.SqrlLogicalTableScan;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import org.apache.calcite.SqrlRelBuilder;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.GroupKey;
import org.apache.calcite.util.ImmutableBitSet;

public class ImplicitKeyPropagator {
  SqrlRelBuilder rel;

  @Value
  static class GroupInformation {
    RelBuilder builder;
    List<Integer> keyPositions;

    public boolean hasKeys() {
      return !keyPositions.isEmpty();
    }
  }

  public GroupInformation propagate(RelNode node) {
    return propagate(node, new PropagateContext(false));
  }

  @AllArgsConstructor
  @Getter
  @Setter
  class PropagateContext {
    boolean limitOnAncestor = false;
  }

  public GroupInformation propagate(RelNode node, PropagateContext context) {
    if (node instanceof Project) {
      Project project = (Project) node;
      GroupInformation info = propagate(project.getInput(), context);

      RelBuilder builder = info.getBuilder();
      if (info.hasKeys()) {
        List<RexNode> projects = new ArrayList<>();
        projects.addAll(project.getProjects());

        for (Integer i : info.getKeyPositions()) {
          projects.add(RexInputRef.of(i, builder.peek().getRowType()));
        }

        builder.project(projects, project.getRowType().getFieldNames());

        List<Integer> newKeyIndex = IntStream.range(projects.size() - info.getKeyPositions().size(), projects.size()).boxed()
            .collect(Collectors.toList());
        return new GroupInformation(info.builder, newKeyIndex);
      } else {
        builder.push(project);
      }

      return new GroupInformation(info.builder, List.of());
    } else if (node instanceof TableScan) {
      SqrlLogicalTableScan scan = (SqrlLogicalTableScan) node;
      RelOptTable table = node.getTable();

      List<Integer> keyIndex = RelColumnTranslator.getColumnIndices(
          scan.getPrimaryKeys(), table.getRowType());

      Preconditions.checkState(scan.getPrimaryKeys().size() != 0, "There should be keys..");
      Preconditions.checkState(keyIndex.size() == scan.getPrimaryKeys().size(), "Could not find keys");

      RelBuilder relBuilder = createOrGetBuilder(
          scan.getCluster(), table.getRelOptSchema());

      relBuilder.push(node);

      return new GroupInformation(relBuilder, keyIndex);
    } else if (node instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) node;

      GroupInformation info = propagate(aggregate.getInput(), context);

      RelBuilder builder = info.getBuilder();
      ImmutableBitSet keyIndexes = ImmutableBitSet.of(info.getKeyPositions());
      ImmutableBitSet allGroups = aggregate.getGroupSet().union(keyIndexes);
      GroupKey groupKey = builder.groupKey(allGroups);
      List<AggCall> calls = aggregate.getAggCallList().stream()
          .map(builder::aggregateCall)
          .collect(Collectors.toList());

      if (context.isLimitOnAncestor() && info.hasKeys()) {
        throw new RuntimeException("ROW_NUM partition tbd");
      }

      builder.aggregate(groupKey, calls);

      //get new key indexes
      List<Integer> newKeyIndex = new ArrayList<>();
      List<Integer> groups = allGroups.asList();
      for (int i = 0; i < groups.size(); i++) {
        int val = groups.get(i);
        if (keyIndexes.get(val)) {
          newKeyIndex.add(i);
        }
      }
      Preconditions.checkState(keyIndexes.asList().size() == newKeyIndex.size());

      return new GroupInformation(info.builder, newKeyIndex);
    } else if (node instanceof Sort) {
      Sort sort = (Sort) node;
      if (sort.fetch != null) {
        context.setLimitOnAncestor(true);
      }

      GroupInformation info = propagate(sort.getInput(), context);
      if (info.hasKeys()) {
        //Step 1: Add keys
        List<RexNode> newSort = new ArrayList<>();
        newSort.addAll(sort.getSortExps());
        //TODO: Fix direction
        for (Integer i : info.getKeyPositions()) {
          newSort.add(RexInputRef.of(i, getBuilder().peek().getRowType()));
        }

        if (sort.fetch != null) {
          getBuilder().sortLimit(0, ((RexLiteral)sort.fetch).getValueAs(Integer.class), newSort);
        } else {
          getBuilder().sort(newSort);
        }
        //todo: wrong
        List<Integer> newKeyIndex = IntStream.range(0, info.getKeyPositions().size() - 1).boxed()
            .collect(Collectors.toList());


        return new GroupInformation(info.builder, newKeyIndex);
      } else {
        getBuilder().push(sort);
      }
      return new GroupInformation(info.builder, List.of());
    } else if (node instanceof Join) {
      Join join = (Join) node;

      GroupInformation left = propagate(join.getLeft(), context);
      GroupInformation right = propagate(join.getRight(), context);

      RelBuilder builder = getBuilder();
      builder.join(join.getJoinType(), join.getCondition(), join.getVariablesSet());

      return new GroupInformation(left.builder, List.of(1));
    } else if (node instanceof LogicalFilter) {
      LogicalFilter filter = (LogicalFilter) node;

      GroupInformation info = propagate(filter.getInput(), context);
      getBuilder().filter(filter.getVariablesSet(), filter.getCondition());
      return info;
    }

    throw new RuntimeException("Unknown: " + node.getClass().getName());
  }

  private RelBuilder getBuilder() {
    Preconditions.checkNotNull(rel);
    return rel;
  }

  private RelBuilder createOrGetBuilder(RelOptCluster cluster, RelOptSchema relOptSchema) {
    if (this.rel == null) {
      this.rel = new SqrlRelBuilder(null, cluster, relOptSchema);
    }

    return rel;
  }
}

