package ai.datasqrl.flink;

import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.util.TimeAnnotatedRecord;
import ai.datasqrl.physical.stream.flink.ProcessError;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FlinkSerialization {

    @Test
    @Disabled
    public void testPojoSerialization() {
        List<Class> streamDataTypes = List.of(TimeAnnotatedRecord.class,
                ProcessError.class, SourceRecord.Raw.class, SourceRecord.Named.class);
        for (Class clazz : streamDataTypes) {
            //Use PojoTestUtils once we are at Flink version 1.17 to verify
            //that the classes we use in the DataStream are valid Pojos
        }
    }

}
