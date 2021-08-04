package ai.dataeng.sqml.source;

import javax.annotation.Nonnull;

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

}
