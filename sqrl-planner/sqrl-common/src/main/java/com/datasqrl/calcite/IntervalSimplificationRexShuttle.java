package com.datasqrl.calcite;

import lombok.AllArgsConstructor;
import org.apache.calcite.rex.*;

import java.math.BigDecimal;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

@AllArgsConstructor
public class IntervalSimplificationRexShuttle extends RexShuttle {
    RexBuilder rexBuilder;
    @Override
    public RexNode visitCall(RexCall call) {
        if (call.getKind() == SqlKind.TIMES) {
            RexNode left = call.operands.get(0);
            RexNode right = call.operands.get(1);

            if (left instanceof RexLiteral
                && ((RexLiteral) left).getTypeName() == SqlTypeName.DECIMAL
                && right.getType() instanceof IntervalSqlType) {
                BigDecimal leftValue = ((RexLiteral) left).getValueAs(BigDecimal.class);
                BigDecimal rightValue = ((RexLiteral) right).getValueAs(BigDecimal.class);
//                long multiplier = ((IntervalSqlType) right.getType()).getPrecision();


                BigDecimal resultValue = leftValue.multiply(rightValue);
                return rexBuilder.makeIntervalLiteral(resultValue, ((IntervalSqlType)right.getType()).getIntervalQualifier());
            }
        }
        return super.visitCall(call);
    }
}
