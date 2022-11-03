package ai.datasqrl.schema;

import ai.datasqrl.schema.type.basic.*;
import ai.datasqrl.util.SnapshotTest;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TypeTest {

    SnapshotTest.Snapshot snapshot;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        this.snapshot = SnapshotTest.Snapshot.of(getClass(),testInfo);
    }

    @Test
    public void printCombinationMatrix() {
        for (Map.Entry<Pair<BasicType,BasicType>, Pair<BasicType,Integer>> entry : BasicTypeManager.TYPE_COMBINATION_MATRIX.entrySet()) {
            Pair<BasicType,BasicType> types = entry.getKey();
            Pair<BasicType,Integer> result = entry.getValue();
            snapshot.addContent(String.format("%s [%d]", result.getKey(), result.getValue()),
                    String.format("%s + %s",types.getKey(),types.getValue()));
        }
        snapshot.createOrValidate();
    }

    @Test
    public void testBasicTypes() {
        assertEquals(7,BasicTypeManager.ALL_TYPES.length);
        assertEquals(FloatType.INSTANCE,BasicTypeManager.combine(IntegerType.INSTANCE,FloatType.INSTANCE,10).get());
        assertEquals(StringType.INSTANCE,BasicTypeManager.combine(IntegerType.INSTANCE,DateTimeType.INSTANCE,50).get());
        assertEquals(IntervalType.INSTANCE,BasicTypeManager.combine(IntegerType.INSTANCE,IntervalType.INSTANCE,40).get());
    }

}
