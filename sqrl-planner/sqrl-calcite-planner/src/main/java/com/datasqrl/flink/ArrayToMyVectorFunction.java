package com.datasqrl.flink;

import com.datasqrl.calcite.type.MyVectorType;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Optional;

public class ArrayToMyVectorFunction extends ScalarFunction {
    public MyVectorType eval(double[] array) {
        return new MyVectorType(array, null, null);
    }


    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(
                        callContext ->
                                Optional.of(
                                        DataTypes.of(MyVectorTypeInfo.INSTANCE)
                                                .toDataType(typeFactory)))
                .build();
    }

    //copied from flink-ml, is there a better way to get this?
    static class MyVectorTypeInfo extends TypeInformation<MyVectorType> {
        private static final long serialVersionUID = 1L;

        public static final MyVectorTypeInfo INSTANCE = new MyVectorTypeInfo();

        public MyVectorTypeInfo() {}

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        public int getTotalFields() {
            return 1;
        }

        @Override
        public Class<MyVectorType> getTypeClass() {
            return MyVectorType.class;
        }

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<MyVectorType> createSerializer(ExecutionConfig executionConfig) {
//            return new DenseVectorSerializer();
            throw new RuntimeException("Type serializer not yet implemented");
        }

        // --------------------------------------------------------------------------------------------

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof MyVectorTypeInfo;
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof MyVectorTypeInfo;
        }

        @Override
        public String toString() {
            return "DenseVectorType";
        }
    }
}