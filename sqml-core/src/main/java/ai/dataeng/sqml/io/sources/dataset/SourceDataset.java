package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.tree.name.Name;

import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A {@link SourceDataset} defines a group of {@link SourceTable} that comprise one semantically coherent dataset.
 *
 * The role of a {@link SourceDataset} is to register all of its {@link SourceTable} within an execution environment.
 *
 * TODO: Run background thread to monitor
 */
public class SourceDataset {

    final DatasetRegistry registry;
    final PollTablesTimer polling;
    private DataSourceConfiguration config;
    private final Map<Name,SourceTable> tables = new HashMap<>();

    public SourceDataset(DatasetRegistry registry, DataSourceConfiguration config) {
        this.registry = registry;
        this.config = config;
        initializeTables();
        polling = new PollTablesTimer();
    }

    void initializeTables() {
        //Read existing tables within dataset from store
        for (SourceTableConfiguration tblConfig : registry.persistence.getTables(config));
    }

    synchronized SourceTable initiateTable(SourceTableConfiguration tableConfig) {
        Preconditions.checkArgument(!tables.containsKey(tableConfig.getTableName()));
        SourceTable tbl = new SourceTable(this,tableConfig);
        tables.put(tbl.getName(),tbl);
        return tbl;
    }

    synchronized void addOrUpdateTable(SourceTableConfiguration tableConfig) {
        SourceTable existingTbl = tables.get(tableConfig.getTableName());
        if (existingTbl == null) {
            //New table
            SourceTable tbl = initiateTable(tableConfig);
            registry.persistence.putTable(config.getDatasetName(),tableConfig);
            registry.tableMonitor.startTableMonitoring(tbl);
        } else {
            existingTbl.updateConfiguration(tableConfig);
        }
    }

    void updateConfiguration(DataSourceConfiguration dsConfig) {
        Preconditions.checkArgument(config.getDatasetName().equals(dsConfig.getDatasetName()));
        //No need to do anything if configurations haven't changed
        if (!config.equals(dsConfig)) {
            //Make sure the new table configuration is compatible and update
            if (config.isCompatible(dsConfig)) {
                config = dsConfig;
                registry.persistence.putDataset(config);
            } else {
                throw new IllegalStateException(String.format("Updated source is incompatible with existing one: [%s]",dsConfig));
            }
        }
    }

    public @NonNull DataSourceConfiguration getConfiguration() {
        return config;
    }

    /**
     * Returns all tables currently in the dataset
     * @return
     */
    public Collection<SourceTable> getTables() {
        return tables.values();
    }

    /**
     * Returns {@link SourceTable} of the given name in this dataset or NULL if such does not exist
     * @param name
     * @return
     */
    public SourceTable getTable(Name name) {
        return tables.get(name);
    }

    public SourceTable getTable(String name) {
        return getTable(toName(name));
    }

    public boolean containsTable(String name) {
        return getTable(name)!=null;
    }

    public Name getName() {
        return config.getDatasetName();
    }

    public NameCanonicalizer getCanonicalizer() {
        return config.getCanonicalizer();
    }

    public Name toName(String s) {
        return Name.of(s, config.getCanonicalizer());
    }

    class PollTablesTimer extends TimerTask {

        @Override
        public void run() {
            Collection<? extends SourceTableConfiguration> tables = Collections.EMPTY_SET;
            try {
                tables = config.pollTables(registry.pollingWaitTimeMS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                //Ignore and try again next time
            } catch (Throwable e) {
                System.err.println("Encountered exception polling from source: " + e); //TODO: log this
            }
            for (SourceTableConfiguration tblConfig: tables) addOrUpdateTable(tblConfig);
            //TODO: Should we implement table removal if table hasn't been present for X amount of time?
        }
    }

    public Digest getDigest() {
        return new Digest(getName(),getCanonicalizer());
    }

    @Value
    public static class Digest {

        private final Name name;
        private final NameCanonicalizer canonicalizer;

    }

}
