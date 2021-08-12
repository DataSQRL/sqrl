package ai.dataeng.sqml.dag;

import lombok.NonNull;

import java.util.List;

/**
 * Defines the fully qualified name of a table.
 *
 * A table is uniquely identified by the dataset in which it is defined and the name of the table within the dataset.
 * Since tables can be nested, the fully qualified name includes all parent table names and is therefore a list of strings.
 *
 * The dataset of a table can be a source dataset or an imported dataset defined in another SQML script.
 * Tables defined in the SQML script or imported with a fully qualified import (such as `IMPORT dataset.tablename`) are
 * local definitions and hence have the current SQML script as their dataset.
 *
 * In SQML, names are not case-sensitive. Hence, any implementation of comparison or hashing of names should be
 * case-insensitive.
 *
 */
public interface QualifiedTableName {

    /**
     *
     * @return The dataset name in which this table is defined
     */
    public @NonNull String getDatasetName();

    /**
     *
     * @return The full name of the table (without dataset)
     */
    public @NonNull List<String> getTableName();

    public default boolean isLocalTo(@NonNull String datasetName) {
        return datasetName.equalsIgnoreCase(getDatasetName());
    }

}
