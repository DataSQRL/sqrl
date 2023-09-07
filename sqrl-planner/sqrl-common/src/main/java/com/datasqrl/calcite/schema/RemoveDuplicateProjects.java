package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlRelBuilder;
import com.datasqrl.calcite.schema.RemoveDuplicateProjects.Context;
import com.datasqrl.calcite.schema.RemoveDuplicateProjects.Result;
import com.datasqrl.calcite.visitor.AbstractRelNodeVisitor;
import com.datasqrl.calcite.visitor.RelRewriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Value;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Resizes the relnode to get rid of extra variables added by the lateral decorrelation
 */
public class RemoveDuplicateProjects extends AbstractRelNodeVisitor<Result, Context> {

  private final QueryPlanner planner;

  public RemoveDuplicateProjects(QueryPlanner planner) {
    this.planner = planner;
  }

  @Override
  public Result visitProject(Project node, Context context) {
    Result result = AbstractRelNodeVisitor.accept(this, node.getInput(), context);

    Shifts i = result.getIndexMap();
    List<RexNode> shifted = applyMapping(node.getProjects(), i);

    Map<RexNode, Integer> mapping = new LinkedHashMap<>();
    List<Integer> newIndex = new ArrayList<>();
    List<String> names = new ArrayList<>();
    for (int j = 0; j < shifted.size(); j++) {
      RexNode rexNode = shifted.get(j);
      if (mapping.containsKey(rexNode)) {
        int key = mapping.get(rexNode);
        newIndex.add(mapping.get(rexNode));
        names.add(node.getRowType().getFieldList().get(j).getName());
      } else {
        mapping.put(rexNode, j);
        newIndex.add(j);
        names.add(node.getRowType().getFieldList().get(j).getName());
      }
    }
//    List<RexNode> s = applyMapping(shifted, new Shifts(newIndex));

//    List<RexNode> moreShifted = applyMapping(shifted, new Shifts(newIndex));



//    SqrlRelBuilder relBuilder = planner.getSqrlRelBuilder();
//    RelNode relNode = relBuilder.push(result.getNewRel())
//        .project(node, node.getRowType().getFieldNames())
//        .hints(node.getHints())
//        .build();

    return new Result((RelNode) node, node, new Shifts(newIndex));
  }

  @Override
  public Result visitCorrelate(Correlate node, Context context) {
    List<RelNode> inputs = new ArrayList<>();
    Result left = AbstractRelNodeVisitor.accept(this, node.getLeft(), context);
    Result right = AbstractRelNodeVisitor.accept(this, node.getRight(), context);
    inputs.add(left.getNewRel());
    inputs.add(right.getNewRel());

    return new Result(node, node.copy(node.getTraitSet(), inputs),
        left.indexMap.append(right.getIndexMap()));
  }

  private List<RexNode> applyMapping(List<RexNode> nodes, Shifts shifts) {
    return nodes.stream()
        .map(n-> applyMapping(n, shifts))
        .collect(Collectors.toList());
  }

  private RexNode applyMapping(RexNode node, Shifts toRemove) {
    return node.accept(new RexShuttle(){
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        int mappedIndex = toRemove.get(inputRef.getIndex());

        return new RexInputRef(mappedIndex, inputRef.getType());
      }
    });
  }

  @Value
  public static class Shift {
    int index;
    int newIndex;
  }

  @Override
  public Result visitJoin(Join node, Context context) {
    Result left = AbstractRelNodeVisitor.accept(this, node.getLeft(), context);
    Result right = AbstractRelNodeVisitor.accept(this, node.getRight(), context);

    Shifts newShifts = left.getIndexMap().append(right.indexMap);

    RelNode relNode = node.copy(node.getTraitSet(),
        applyMapping(node.getCondition(), newShifts),
        left.getNewRel(),
        right.getNewRel(),
        node.getJoinType(),
        node.isSemiJoin());
    return new Result((RelNode) node, relNode, newShifts);
  }

  @Override
  public Result visitAggregate(Aggregate node, Context context) {
    Result result = AbstractRelNodeVisitor.accept(this, node.getInput(), context);

    return new Result((RelNode) node, node.copy(node.getTraitSet(),
        result.getNewRel(),
        applyMapping(node.getGroupSet(), result.getIndexMap()),
        node.getGroupSets(),
        node.getAggCallList()), new Shifts(IntStream.range(0, node.getRowType().getFieldCount())
        .boxed().collect(
        Collectors.toList())));
  }

  private ImmutableBitSet applyMapping(ImmutableBitSet groupSet, Shifts toRemove) {
    List<Integer> i = groupSet.asList().stream()
        .map(toRemove::get)
        .collect(Collectors.toList());
    return ImmutableBitSet.of(i);
  }

  @Override
  public Result visitFilter(Filter node, Context context) {
    Result result = AbstractRelNodeVisitor.accept(this, node.getInput(), context);

    return new Result((RelNode) node, node.copy(node.getTraitSet(),
        result.getNewRel(),
        applyMapping(node.getCondition(), result.getIndexMap())),
        result.getIndexMap());
  }



  @Override
  public Result visitUnion(Union node, Context context) {

    return new Result((RelNode) node, node.copy(node.getTraitSet(),
        node.getInputs().stream()
            .map(n-> RelRewriter.accept(this, n, context).newRel)
            .collect(Collectors.toList()))
        , null);//todo nested unions
  }

  @Override
  public Result visitSort(Sort node, Context context) {
    Result result = RelRewriter.accept(this, node.getInput(0), context);
    final RelCollation existingCollations =
        RelCollationTraitDef.INSTANCE.canonize(
            RelCollations.permute(node.getCollation(), result.getIndexMap().asMap()));

    List<RelFieldCollation> collations = new ArrayList<>();
    collations.add(new RelFieldCollation(0));
    collations.addAll(existingCollations.getFieldCollations());
    RelCollation collation = RelCollations.of(collations);
    return new Result((RelNode) node, node.copy(node.getTraitSet().replace(collation),
        result.getNewRel(),
        collation,
        node.offset,
        node.fetch
    ), result.indexMap);
  }

  @Override
  public Result visitTableScan(TableScan node, Context context) {
    return new Result((RelNode) node, node, new Shifts(IntStream.range(0,
        node.getRowType().getFieldCount()).boxed().collect(Collectors.toList())));
  }

  @Value
  public static class Result {
    private final RelNode newRel;
    private final Shifts indexMap;

    public Result(RelNode node, RelNode newRel, Shifts indexMap) {
      this.newRel = newRel;
      this.indexMap = indexMap;

    }
  }
  public static class Context {

  }


  private class Shifts {
    private final List<Integer> newIndex;

    public Shifts(List<Integer> newIndex) {

      this.newIndex = newIndex;
    }

    public int get(int index) {
      return newIndex.get(index);
    }

    public Shifts append(Shifts indexMap) {
      List<Integer> mapping = new ArrayList<>(this.newIndex);
      for (Integer i : indexMap.newIndex) {
        mapping.add( i+ this.newIndex.size());
      }
      return new Shifts(mapping);
    }

    public Map<Integer, Integer> asMap() {
      Map<Integer, Integer> map = new HashMap<>();
      for (int i = 0; i < this.newIndex.size(); i++) {
        map.put(i, this.newIndex.get(i));
      }

      return map;
    }
  }
}
