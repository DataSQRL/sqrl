package com.datasqrl.calcite.convert;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

public class IntervalSimplificationShuttle extends RelShuttleImpl {

    @Override
    public RelNode visit(LogicalProject project) {
        List<RexNode> modifiedProjects = new ArrayList<>();
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        boolean changed = false;

        for (RexNode expr : project.getProjects()) {
            if (expr.getKind() == SqlKind.TIMES) {
                RexCall multiplication = (RexCall) expr;
                RexNode left = multiplication.operands.get(0);
                RexNode right = multiplication.operands.get(1);

                if (left instanceof RexLiteral && ((RexLiteral) left).getTypeName() == SqlTypeName.DECIMAL
                        && right.getType() instanceof IntervalSqlType) {
                    BigDecimal leftValue = ((RexLiteral) left).getValueAs(BigDecimal.class);
                    long multiplier = ((IntervalSqlType)right.getType()).getPrecision();

                    BigDecimal resultValue = leftValue.multiply(BigDecimal.valueOf(multiplier));

                    RexNode newLiteral = rexBuilder.makeExactLiteral(resultValue);
                    modifiedProjects.add(newLiteral);
                    changed = true;
                } else {
                    modifiedProjects.add(expr);
                }
            } else {
                modifiedProjects.add(expr);
            }
        }

        if (changed) {
            return project.copy(project.getTraitSet(), project.getInput(), modifiedProjects, project.getRowType());
        } else {
            return super.visit(project);
        }
    }
}
