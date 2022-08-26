package ai.datasqrl.plan.calcite.util;

import ai.datasqrl.function.SqrlAwareFunction;
import com.google.common.base.Preconditions;
import lombok.Value;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

@Value
public class TimeBucketFunctionCall {

    private final SqrlAwareFunction operator;
    private final int timestampColumnIndex;
    //TODO: normalize other arguments to single INTERVAL constants


    public static TimeBucketFunctionCall from(RexCall call) {
        Preconditions.checkArgument(call.getOperator() instanceof SqrlAwareFunction);
        SqrlAwareFunction bucketFct = (SqrlAwareFunction) call.getOperator();
        Preconditions.checkArgument(bucketFct.isTimeBucketingFunction());
        //Validate time bucketing function: First argument is timestamp, all others must be constants
        Preconditions.checkArgument(call.getOperands().size()>0,"Time-bucketing function must have at least one argument");
        RexNode timestamp = call.getOperands().get(0);
        Preconditions.checkArgument(CalciteUtil.isTimestamp(timestamp.getType()),"Expected timestamp argument");
        Preconditions.checkArgument(timestamp instanceof RexInputRef);
        int timeColIndex = ((RexInputRef)timestamp).getIndex();
        for (int i = 1; i < call.getOperands().size(); i++) {
            RexNode operand = call.getOperands().get(i);
            Preconditions.checkArgument(RexUtil.isConstant(operand),"All non-timestamp arguments must be constants");
        }
        return new TimeBucketFunctionCall(bucketFct,timeColIndex);
    }

}
