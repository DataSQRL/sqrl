package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.execution.flink.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.flink.streaming.api.datastream.DataStream;

import static ai.dataeng.sqml.io.sources.dataset.DatasetRegistry.*;

/**
 * A {@link SourceTable} defines an input source to be imported into an SQML script. A {@link SourceTable} is comprised
 * of records and is the smallest unit of data that one can refer to within an SQML script.
 */
public class SourceTable {

    final SourceDataset dataset;
    private SourceTableConfiguration config;

    public SourceTable(SourceDataset dataset, SourceTableConfiguration config) {
        this.dataset = dataset;
        this.config = config;
    }

    void updateConfiguration(SourceTableConfiguration tableConfig) {
        Preconditions.checkArgument(tableConfig.getTableName().equals(config.getTableName()));
        //No need to do anything if configurations haven't changed
        if (!config.equals(tableConfig)) {
            //Make sure the new table configuration is compatible and update
            if (config.isCompatible(tableConfig)) {
                config = tableConfig;
                dataset.registry.persistence.putTable(dataset.getName(), tableConfig);
            } else {
                throw new IllegalStateException(String.format("Updated table is incompatible with existing one: [%s]",tableConfig));
            }
        }
    }

    public @NonNull SourceTableConfiguration getConfiguration() {
        return config;
    }


    /**
     *
     * @return {@link SourceDataset} that this table is part of
     */
    public SourceDataset getDataset() {
        return dataset;
    }

    /**
     * Returns the name of this table. It must be unique within its {@link SourceDataset}
     * @return
     */
    public Name getName() {
        return config.getTableName();
    }

    public boolean hasSchema() {
        return false;
    }

    /**
     * Produces a {@link DataStream} of {@link SourceRecord} for this table source.
     *
     * TODO: Need to figure out how to distinguish between replay from start vs continuous streaming
     *
     * @return
     */
    //public DataStream<SourceRecord<String>> getDataStream(StreamExecutionEnvironment env);

    public SourceTableStatistics getStatistics() {
        SourceTableStatistics stats = dataset.registry.persistence.getTableStatistics(dataset.getName(),getName());
        if (stats == null) stats = new SourceTableStatistics();
        return stats;
    }

}
