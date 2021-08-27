package ai.dataeng.sqml.ingest.source;

import javax.annotation.Nonnull;
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
    public void addSourceTableListener(@Nonnull SourceTableListener listener);

    /**
     * Each {@link SourceDataset} is uniquely identified by a name within an execution environment. Datasets are
     * imported within an SQML script by that name.
     *
     * @return Unique name of this source dataset
     */
    public String getName();

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
    public SourceTable getTable(String name);

    public default boolean containsTable(String name) {
        return getTable(name)!=null;
    }

}
