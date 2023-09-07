package com.datasqrl.calcite.visitor;

import com.datasqrl.plan.rel.LogicalStream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;

public abstract class AbstractRelNodeVisitor<R, C> implements RelNodeVisitor<R, C> {

  public static <R, C> R accept(RelNodeVisitor<R, C> visitor, RelNode relNode, C context) {
    if (relNode instanceof TableScan) {
      return visitor.visitTableScan((TableScan) relNode, context);
    } else if (relNode instanceof Aggregate) {
      return visitor.visitAggregate((Aggregate) relNode, context);
    }  else if (relNode instanceof Join) {
      return visitor.visitJoin((Join) relNode, context);
    } else if (relNode instanceof Filter) {
      return visitor.visitFilter((Filter) relNode, context);
    } else if (relNode instanceof Project) {
      return visitor.visitProject((Project) relNode, context);
    } else if (relNode instanceof Intersect) {
      return visitor.visitIntersect((Intersect) relNode, context);
    } else if (relNode instanceof Minus) {
      return visitor.visitMinus((Minus) relNode, context);
    } else if (relNode instanceof Values) {
      return visitor.visitValues((Values) relNode, context);
    } else if (relNode instanceof LogicalStream) {
      return visitor.visitLogicalStream((LogicalStream) relNode, context);
    } else if (relNode instanceof Snapshot) {
      return visitor.visitSnapshot((Snapshot) relNode, context);
    } else if (relNode instanceof TableFunctionScan) {
      return visitor.visitTableFunctionScan((TableFunctionScan) relNode, context);
    } else if (relNode instanceof Correlate) {
      return visitor.visitCorrelate((Correlate) relNode, context);
    } else if (relNode instanceof Calc) {
      return visitor.visitCalc((Calc) relNode, context);
    } else if (relNode instanceof Exchange) {
      return visitor.visitExchange((Exchange) relNode, context);
    } else if (relNode instanceof TableModify) {
      return visitor.visitTableModify((TableModify) relNode, context);
    } else if (relNode instanceof Match) {
      return visitor.visitMatch((Match) relNode, context);
    } else if (relNode instanceof Union) {
      return visitor.visitUnion((Union) relNode, context);
    } else if (relNode instanceof Sort) {
      return visitor.visitSort((Sort) relNode, context);
    } else {
      return visitor.visitRelNode(relNode, context);
    }
  }

  @Override
  public R visitTableScan(TableScan node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitJoin(Join node, C context) {
    return visitRelNode(node, context);
  }
  public R visitAggregate(Aggregate node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitFilter(Filter node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitProject(Project node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitIntersect(Intersect node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitMinus(Minus node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitValues(Values node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitLogicalStream(LogicalStream node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitSnapshot(Snapshot node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitTableFunctionScan(TableFunctionScan node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitCorrelate(Correlate node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitCalc(Calc node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitExchange(Exchange node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitTableModify(TableModify node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitMatch(Match node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitUnion(Union node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitSort(Sort node, C context) {
    return visitRelNode(node, context);
  }

  @Override
  public R visitRelNode(RelNode node, C context) {
    throw new RuntimeException("Unrecognized node:" + node);
  }
}