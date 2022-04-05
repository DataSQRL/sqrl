package ai.dataeng.sqml.io.sinks;

import ai.dataeng.sqml.tree.name.Name;
import lombok.Value;

@Value
public class TableSink {

    private final Name name;
    private final DataSink sink;

}
