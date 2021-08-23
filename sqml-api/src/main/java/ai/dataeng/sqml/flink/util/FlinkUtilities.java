package ai.dataeng.sqml.flink.util;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.ingest.NamePath;
import ai.dataeng.sqml.ingest.RecordShredder;
import ai.dataeng.sqml.ingest.SourceTableSchema;
import ai.dataeng.sqml.type.*;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class FlinkUtilities {

    public static final Random random = new Random();

    public static int generateBalancedKey(final int parallelism) {
        return random.nextInt(parallelism<<8);
    }

    public static<T> KeySelector<T,Integer> getSingleKeySelector(final int key) {
        return new KeySelector<T, Integer>() {
            @Override
            public Integer getKey(T t) throws Exception {
                return key;
            }
        };
    }

    public static long getCurrentProcessingTime() {
        return System.currentTimeMillis();
    }

    public static void enableCheckpointing(StreamExecutionEnvironment env) {
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    }

    public static<T> KeySelector<T, Integer> getHashPartitioner(final int parallelism) {
        final int modulus = parallelism;
        return new KeySelector<T, Integer>() {
            @Override
            public Integer getKey(T sourceRecord) throws Exception {
                return sourceRecord.hashCode()%modulus;
            }
        };
    }

    public static final ObjectArrayTypeInfo INSTANT_ARRAY_TYPE_INFO = ObjectArrayTypeInfo.getInfoFor(Instant[].class, BasicTypeInfo.INSTANT_TYPE_INFO);


    public static TypeInformation getFlinkTypeInfo(ScalarType datatype, boolean isArray) {
        if (datatype instanceof StringType) {
            if (isArray) return BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.STRING_TYPE_INFO;
        } else if (datatype instanceof IntegerType) {
            if (isArray) return BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.LONG_TYPE_INFO;
        } else if (datatype instanceof FloatType) {
            if (isArray) return BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.DOUBLE_TYPE_INFO;
        } else if (datatype instanceof NumberType) {
            if (isArray) return BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.DOUBLE_TYPE_INFO;
        } else if (datatype instanceof BooleanType) {
            if (isArray) return BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.BOOLEAN_TYPE_INFO;
        } else if (datatype instanceof UuidType) {
            if (isArray) return BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.STRING_TYPE_INFO;
        } else if (datatype instanceof DateTimeType) { //TODO: need to make more robust!
            if (isArray) return INSTANT_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.INSTANT_TYPE_INFO;
        } else {
            throw new IllegalArgumentException("Unrecognized data type: " + datatype);
        }
    }

    public static RowTypeInfo convert2RowTypeInfo(DestinationTableSchema schema) {
        TypeInformation[] colTypes = new TypeInformation[schema.length()];
        String[] colNames = new String[schema.length()];

        for (int i = 0; i < schema.length(); i++) {
            DestinationTableSchema.Field field = schema.get(i);
            colNames[i]= field.getName();
            colTypes[i]= getFlinkTypeInfo(field.getType(), field.isArray());

        }
        return new RowTypeInfo(colTypes,colNames);
    }

}
