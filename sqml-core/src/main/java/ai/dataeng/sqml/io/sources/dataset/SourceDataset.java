package ai.dataeng.sqml.io.sources.dataset;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

/**
 * A {@link SourceDataset} defines a group of {@link SourceTable} that comprise one semantically coherent dataset.
 *
 * The role of a {@link SourceDataset} is to register all of its {@link SourceTable} within an execution environment.
 *
 */
public class SourceDataset {

    final DatasetRegistry registry;
    private DataSource source;
    private final Map<Name,SourceTable> tables = new HashMap<>();

    public SourceDataset(DatasetRegistry registry, DataSource source) {
        this.registry = registry;
        this.source = source;
        initializeTables();
    }

    void initializeTables() {
        //Read existing tables within dataset from store
        for (SourceTableConfiguration tblConfig : registry.persistence.getTables(source.getDatasetName())) {
            ProcessMessage.ProcessBundle<ConfigurationError> errors = new ProcessMessage.ProcessBundle<>();
            SourceTable table = initiateTable(tblConfig, errors);
            registry.tableMonitor.startTableMonitoring(table);
            ProcessMessage.ProcessBundle.logMessages(errors);
        }
    }

    synchronized SourceTable initiateTable(SourceTableConfiguration tableConfig,
                                           ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        Name tblName = getCanonicalizer().name(tableConfig.getName());
        Preconditions.checkArgument(!tables.containsKey(tblName));
        SourceTable tbl = new SourceTable(this, tblName, tableConfig);
        tables.put(tblName,tbl);
        return tbl;
    }

    synchronized SourceTable addTable(SourceTableConfiguration tableConfig,
                               ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableConfig.getName()));
        Name tblName = Name.of(tableConfig.getName(),getCanonicalizer());
        SourceTable table = tables.get(tblName);
        if (table == null) {
            //New table
            if (tableConfig.validateAndInitialize(this.getSource(),errors)) {
                table = initiateTable(tableConfig, errors);
            } else {
                return null;
            }
        } else {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,getName(),
                    "Table [%s] already exists. To update table, delete and re-add", tblName.getDisplay()));
            return null;
        }
        registry.persistence.putTable(source.getDatasetName(), tblName, tableConfig);
        registry.tableMonitor.startTableMonitoring(table);
        return table;
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


    public Digest getDigest() {
        return new Digest(getName(),getCanonicalizer());
    }

    @Value
    public static class Digest implements Serializable {

        private final Name name;
        private final NameCanonicalizer canonicalizer;

    }

}
