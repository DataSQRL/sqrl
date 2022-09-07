package ai.datasqrl.plan.calcite.util;

import ai.datasqrl.function.SqrlTimeTumbleFunction;
import com.google.common.base.Preconditions;
import lombok.Value;
import org.apache.calcite.rex.*;

import java.util.ArrayList;
import java.util.List;

@Value
public class TimeTumbleFunctionCall {

    private final SqrlTimeTumbleFunction operator;
    private final int timestampColumnIndex;
    private final long[] arguments;

    public SqrlTimeTumbleFunction.Specification getSpecification() {
        return operator.getSpecification(arguments);
    }

    public static TimeTumbleFunctionCall from(RexCall call, RexBuilder rexBuilder) {
        Preconditions.checkArgument(call.getOperator() instanceof SqrlTimeTumbleFunction);
        SqrlTimeTumbleFunction bucketFct = (SqrlTimeTumbleFunction) call.getOperator();
        //Validate time bucketing function: First argument is timestamp, all others must be constants
        Preconditions.checkArgument(call.getOperands().size()>0,"Time-bucketing function must have at least one argument");
        RexNode timestamp = call.getOperands().get(0);
        Preconditions.checkArgument(CalciteUtil.isTimestamp(timestamp.getType()),"Expected timestamp argument");
        Preconditions.checkArgument(timestamp instanceof RexInputRef);
        int timeColIndex = ((RexInputRef)timestamp).getIndex();
        List<RexNode> operands = new ArrayList<>();
        for (int i = 1; i < call.getOperands().size(); i++) {
            RexNode operand = call.getOperands().get(i);
            Preconditions.checkArgument(RexUtil.isConstant(operand),"All non-timestamp arguments must be constants");
            operands.add(operand);
        }
        long[] operandValues = new long[0];
        if (!operands.isEmpty()) {
            ExpressionReducer reducer = new ExpressionReducer();
            operandValues = reducer.reduce2Long(rexBuilder, operands); //throws exception if arguments cannot be reduced
        }
        return new TimeTumbleFunctionCall(bucketFct, timeColIndex, operandValues);
    }

}
