//package com.datasqrl.time;
//
//import com.datasqrl.error.NotYetImplementedException;
//import com.datasqrl.function.FlinkTypeUtil;
//import com.datasqrl.function.FlinkTypeUtil.VariableArguments;
//import com.datasqrl.function.SqrlFunction;
//import com.datasqrl.function.SqrlTimeTumbleFunction;
//import com.google.common.base.Preconditions;
//import java.time.Instant;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.catalog.DataTypeFactory;
//import org.apache.flink.table.functions.ScalarFunction;
//import org.apache.flink.table.types.inference.TypeInference;
//
//public class EndOfSeconds extends ScalarFunction implements SqrlTimeTumbleFunction {
//
//  @Override
//  public String getDocumentation() {
//    return "Rounds timestamp to the end of the given interval and offset.";
//  }
//
//  public Instant eval(Instant instant, Integer width, Integer offset) {
//    throw new NotYetImplementedException("testing");
//  }
//
//  @Override
//  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
//    return TypeInference.newBuilder().inputTypeStrategy(
//            VariableArguments.builder().staticType(DataTypes.INT()).variableType(DataTypes.INT())
//                .minVariableArguments(0).maxVariableArguments(1).build()).outputTypeStrategy(
//            FlinkTypeUtil.nullPreservingOutputStrategy(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))
//        .build();
//  }
//
//  @Override
//  public Specification getSpecification(long[] arguments) {
//    Preconditions.checkArgument(arguments.length == 1 || arguments.length == 2);
//    return new Specification() {
//      @Override
//      public long getWindowWidthMillis() {
//        return arguments[0] * 1000;
//      }
//
//      @Override
//      public long getWindowOffsetMillis() {
//        return arguments.length > 1 ? arguments[1] * 1000 : 0;
//      }
//    };
//  }
//
//}