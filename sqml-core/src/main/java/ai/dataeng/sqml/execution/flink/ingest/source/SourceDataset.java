package ai.dataeng.sqml.execution.flink.ingest.source;

import ai.dataeng.sqml.execution.flink.ingest.DatasetRegistration;
import ai.dataeng.sqml.tree.name.Name;

import lombok.NonNull;
import java.util.Collection;

/**
 * A {@link SourceDataset} defines a group of {@link SourceTable} that comprise one semantically coherent dataset.
 *
 * The role of a {@link SourceDataset} is to register all of its {@link SourceTable} within an execution environment.
 */
public interface SourceDataset {

    /**
     * Add execution environment to listen for source tables
     * @param listener
     */
    public void addSourceTableListener(@NonNull SourceTableListener listener);

    /**
     * Returns all tables currently in the dataset
     * @return
     */
    public Collection<? extends SourceTable> getTables();

    /**
     * Returns {@link SourceTable} of the given name in this dataset or NULL if such does not exist
     * @param name
     * @return
     */
    public SourceTable getTable(Name name);

    default public SourceTable getTable(String name) {
        return getTable(getRegistration().toName(name));
    }

    public default boolean containsTable(String name) {
        return getTable(name)!=null;
    }

    public DatasetRegistration getRegistration();

    public default Name getName() {
        return getRegistration().getName();
    }

}
