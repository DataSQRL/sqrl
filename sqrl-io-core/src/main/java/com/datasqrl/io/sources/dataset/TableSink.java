package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.io.formats.Format;
import ai.datasqrl.io.formats.FormatConfiguration;
import ai.datasqrl.io.sources.DataSystemConnector;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import lombok.Getter;
import lombok.NonNull;

import java.util.Optional;

@Getter
public class TableSink extends AbstractExternalTable {

  private final Optional<FlexibleDatasetSchema.TableField> schema;


  public TableSink(@NonNull DataSystemConnector dataset, @NonNull TableConfig configuration, @NonNull NamePath path, @NonNull Name name,
                   Optional<FlexibleDatasetSchema.TableField> schema) {
    super(dataset, configuration, path, name);
    this.schema = schema;
  }

  public Format.Writer getWriter() {
    FormatConfiguration formatConfig = configuration.getFormat();
    return formatConfig.getImplementation().getWriter(formatConfig);
  }


}
