package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.tree.name.Name;
import lombok.NonNull;

import java.io.Serializable;

public interface SourceTableConfiguration extends Serializable {

    /**
     * The name of this table.
     * The name must be unique within a dataset
     *
     * @return name of table
     */
    @NonNull Name getTableName();

    /**
     * Returns true if the existing configuration is compatible with the updated one,
     * else false
     * @param otherConfig
     * @return
     */
    boolean isCompatible(SourceTableConfiguration otherConfig);




}
