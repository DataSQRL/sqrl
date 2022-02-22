package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.tree.name.Name;

import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import java.io.Serializable;
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
    private DataSource source;
    private final Map<Name,SourceTable> tables = new HashMap<>();

    public SourceDataset(DatasetRegistry registry, DataSource source) {
        this.registry = registry;
        this.source = source;
        initializeTables();
        polling = new PollTablesTimer(registry.pollingBackgroundWaitTimeMS);
    }

    void initializeTables() {
        //Read existing tables within dataset from store
        for (SourceTableConfiguration tblConfig : registry.persistence.getTables(source.getDatasetName()));
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
            registry.persistence.putTable(source.getDatasetName(),tableConfig);
            registry.tableMonitor.startTableMonitoring(tbl);
        } else {
            existingTbl.updateConfiguration(tableConfig);
        }
    }

    void updateConfiguration(DataSource update) {
        Preconditions.checkArgument(source.getDatasetName().equals(update.getDatasetName()));
        source = update;
        registry.persistence.putDataset(source.getDatasetName(),source.getConfiguration());
    }

    public DataSource getSource() {
        return source;
    }

    public @NonNull DataSource getConfiguration() {
        return source;
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
        return source.getDatasetName();
    }

    public NameCanonicalizer getCanonicalizer() {
        return source.getCanonicalizer();
    }

    public Name toName(String s) {
        return Name.of(s, source.getCanonicalizer());
    }

    void refreshTables(long pollingWaitTimeMS) throws InterruptedException {
        Collection<? extends SourceTableConfiguration> tables = Collections.EMPTY_SET;
        tables = source.pollTables(pollingWaitTimeMS, TimeUnit.MILLISECONDS);
        for (SourceTableConfiguration tblConfig: tables) addOrUpdateTable(tblConfig);
        //TODO: Should we implement table removal if table hasn't been present for X amount of time?
    }

    @AllArgsConstructor
    class PollTablesTimer extends TimerTask {

        private final long pollingWaitTimeMS;

        @Override
        public void run() {
            try {
                refreshTables(pollingWaitTimeMS);
            }  catch (InterruptedException e) {
                //Ignore since we will poll again
                return;
            }
        }
    }

    public Digest getDigest() {
        return new Digest(getName(),getCanonicalizer());
    }

    @Value
    public static class Digest implements Serializable {

        private final Name name;
        private final NameCanonicalizer canonicalizer;

    }

}
