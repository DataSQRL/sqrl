package ai.datasqrl.io.sinks.registry;

import ai.datasqrl.io.sinks.DataSinkRegistration;
import ai.datasqrl.parse.tree.name.Name;

import java.util.Collection;

public interface DataSinkRegistryPersistence {

    Collection<DataSinkRegistration> getSinks();

    void putSink(Name sink, DataSinkRegistration sinkRegistration);

    void removeSink(Name sink);


}
