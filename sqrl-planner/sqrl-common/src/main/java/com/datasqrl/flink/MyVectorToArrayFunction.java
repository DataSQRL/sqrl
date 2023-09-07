package com.datasqrl.flink;

import com.datasqrl.calcite.type.MyVectorType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Optional;

public class MyVectorToArrayFunction extends ScalarFunction {
    public double[] eval(MyVectorType vector) {
      return vector.toArray();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .outputTypeStrategy(
              callContext ->
                  Optional.of(
                      DataTypes.ARRAY(
                          DataTypes.DOUBLE()
                              .notNull()
                              .bridgedTo(double.class))))
          .build();
    }
  }