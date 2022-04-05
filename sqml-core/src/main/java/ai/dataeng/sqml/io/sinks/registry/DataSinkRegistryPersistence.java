package ai.dataeng.sqml.io.sinks.registry;

import ai.dataeng.sqml.io.sinks.DataSinkRegistration;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistryPersistence;
import ai.dataeng.sqml.tree.name.Name;

import java.util.Collection;

public interface DataSinkRegistryPersistence {

    Collection<DataSinkRegistration> getSinks();

    void putSink(Name sink, DataSinkRegistration sinkRegistration);

    void removeSink(Name sink);


}
