package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.SourceTableConfiguration;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;
import lombok.NonNull;

/**
 * A {@link SourceTable} defines an input source to be imported into an SQML script. A {@link
 * SourceTable} is comprised of records and is the smallest unit of data that one can refer to
 * within an SQML script.
 */
public class SourceTable {

  final SourceDataset dataset;
  private final SourceTableConfiguration config;
  private final Name name;

  public SourceTable(SourceDataset dataset, Name tableName, SourceTableConfiguration config) {
    this.dataset = dataset;
    this.config = config;
    this.name = tableName;
  }

  public @NonNull SourceTableConfiguration getConfiguration() {
    return config;
  }


  /**
   * @return {@link SourceDataset} that this table is part of
   */
  public SourceDataset getDataset() {
    return dataset;
  }

  public boolean hasSourceTimestamp() {
    return dataset.getSource().getImplementation().hasSourceTimestamp();
  }

  /**
   * Returns the name of this table. It must be unique within its {@link SourceDataset}
   *
   * @return
   */
  public Name getName() {
    return name;
  }

  public String qualifiedName() {
    return getDataset().getName().getCanonical() + "." + getName().getCanonical();
  }

  public boolean hasSchema() {
    return false;
  }

  public SourceTableStatistics getStatistics() {
    SourceTableStatistics stats = dataset.registry.persistence.getTableStatistics(dataset.getName(),
        getName());
    if (stats == null) {
      stats = new SourceTableStatistics();
    }
    return stats;
  }

}
