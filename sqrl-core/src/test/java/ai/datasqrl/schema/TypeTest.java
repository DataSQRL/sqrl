package ai.datasqrl.schema;

import ai.datasqrl.schema.type.basic.*;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

//TODO: add snapshot testing instead of printout
public class TypeTest {

    @Test
    public void printCombinationMatrix() {
        for (Map.Entry<Pair<BasicType,BasicType>, Pair<BasicType,Integer>> entry : BasicTypeManager.TYPE_COMBINATION_MATRIX.entrySet()) {
            Pair<BasicType,BasicType> types = entry.getKey();
            Pair<BasicType,Integer> result = entry.getValue();
            System.out.printf("%s + %s = %s [%d]\n",types.getKey(),types.getValue(),result.getKey(),result.getValue());
        }
    }

    @Test
    public void testBasicTypes() {
        assertEquals(7,BasicTypeManager.ALL_TYPES.length);
        assertEquals(FloatType.INSTANCE,BasicTypeManager.combine(IntegerType.INSTANCE,FloatType.INSTANCE,10).get());
        assertEquals(StringType.INSTANCE,BasicTypeManager.combine(IntegerType.INSTANCE,DateTimeType.INSTANCE,50).get());
        assertEquals(IntervalType.INSTANCE,BasicTypeManager.combine(IntegerType.INSTANCE,IntervalType.INSTANCE,40).get());
    }

}
