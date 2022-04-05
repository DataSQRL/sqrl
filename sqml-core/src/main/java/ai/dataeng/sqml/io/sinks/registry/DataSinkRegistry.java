package ai.dataeng.sqml.io.sinks.registry;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.error.ErrorPrefix;
import ai.dataeng.sqml.io.sinks.DataSink;
import ai.dataeng.sqml.io.sinks.DataSinkRegistration;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceUpdate;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistryPersistence;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.tree.name.Name;
import lombok.NonNull;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DataSinkRegistry implements DataSinkLookup {

    final DataSinkRegistryPersistence persistence;

    private final Map<Name, DataSink> dataSinks;

    public DataSinkRegistry(DataSinkRegistryPersistence persistence) {
        this.persistence = persistence;
        this.dataSinks = new HashMap<>();
        initializeSinks();
    }

    private void initializeSinks() {
        //Read existing datasets from store
        for (DataSinkRegistration sinkReg : persistence.getSinks()) {
            DataSink sink = new DataSink(sinkReg);
            dataSinks.put(sink.getName(),sink);
        }
    }

    public synchronized DataSink addOrUpdateSink
            (@NonNull DataSinkRegistration sinkReg,
             @NonNull ErrorCollector errors) {
        if (!sinkReg.validateAndInitialize(errors)) return null;
        DataSink sink = new DataSink(sinkReg);
        if (dataSinks.containsKey(sink.getName())) {
            errors.notice("Replacing sink with name: %s", sinkReg.getName());
        }
        dataSinks.put(sink.getName(),sink);
        return sink;
    }

    public synchronized DataSink removeSink(@NonNull Name name) {
        DataSink sink = dataSinks.remove(name);
        if (sink != null) {
            persistence.removeSink(name);
        }
        return sink;
    }

    public DataSink removeSink(@NonNull String name) {
        return Name.getIfValidSystemName(name,this::removeSink);
    }


    @Override
    public DataSink getSink(Name name) {
        return dataSinks.get(name);
    }

    public Collection<DataSink> getSinks() {
        return dataSinks.values();
    }

}
