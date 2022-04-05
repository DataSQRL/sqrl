package ai.dataeng.sqml.io.sinks.registry;

import ai.dataeng.sqml.io.sinks.DataSink;
import ai.dataeng.sqml.tree.name.Name;

public interface DataSinkLookup {

    DataSink getSink(Name name);

    default DataSink getSink(String name) {
        return getSink(Name.system(name));
    }

}
