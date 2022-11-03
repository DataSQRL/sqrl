package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.io.formats.Format;
import ai.datasqrl.io.formats.FormatConfiguration;
import ai.datasqrl.io.sources.DataSourceConnector;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import lombok.NonNull;


public class TableSink extends AbstractExternalTable {

  public TableSink(@NonNull DataSourceConnector dataset, @NonNull TableConfig configuration, @NonNull NamePath path, @NonNull Name name) {
    super(dataset, configuration, path, name);
  }

  public Format.Writer getWriter() {
    FormatConfiguration formatConfig = configuration.getFormat();
    return formatConfig.getImplementation().getWriter(formatConfig);
  }

}
