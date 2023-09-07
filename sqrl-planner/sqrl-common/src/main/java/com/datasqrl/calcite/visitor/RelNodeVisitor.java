package com.datasqrl.calcite.visitor;

import com.datasqrl.plan.rel.LogicalStream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;

public interface RelNodeVisitor<R, C> {
  R visitTableScan(TableScan node, C context);
  R visitAggregate(Aggregate node, C context);
  R visitJoin(Join node, C context);
  R visitFilter(Filter node, C context);
  R visitProject(Project node, C context);
  R visitIntersect(Intersect node, C context);
  R visitMinus(Minus node, C context);
  R visitValues(Values node, C context);
  R visitLogicalStream(LogicalStream node, C context);
  R visitSnapshot(Snapshot node, C context);
  R visitTableFunctionScan(TableFunctionScan node, C context);
  R visitCorrelate(Correlate node, C context);
  R visitCalc(Calc node, C context);
  R visitExchange(Exchange node, C context);
  R visitTableModify(TableModify node, C context);
  R visitMatch(Match node, C context);
  R visitUnion(Union node, C context);
  R visitSort(Sort node, C context);
  R visitRelNode(RelNode node, C context);
}