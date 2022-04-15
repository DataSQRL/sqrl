package ai.datasqrl.io.sinks.registry;

import ai.datasqrl.io.sinks.DataSink;
import ai.datasqrl.parse.tree.name.Name;

public interface DataSinkLookup {

    DataSink getSink(Name name);

    default DataSink getSink(String name) {
        return Name.getIfValidSystemName(name,this::getSink);
    }

}
