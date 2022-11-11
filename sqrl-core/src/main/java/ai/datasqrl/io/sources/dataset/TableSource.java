package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.io.sources.DataSystemConnector;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.InputTableSchema;
import lombok.NonNull;

/**
 * A {@link TableSource} defines an input source to be imported into an SQML script. A {@link
 * TableSource} is comprised of records and is the smallest unit of data that one can refer to
 * within an SQML script.
 */
public class TableSource extends TableInput {

  @NonNull
  private final FlexibleDatasetSchema.TableField schema;

  public TableSource(DataSystemConnector dataset, TableConfig configuration, NamePath path, Name name,
                     FlexibleDatasetSchema.TableField schema) {
    super(dataset, configuration, path, name);
    this.schema = schema;
  }

  public InputTableSchema getSchema() {
    return new InputTableSchema(schema, connector.hasSourceTimestamp());
  }
}
