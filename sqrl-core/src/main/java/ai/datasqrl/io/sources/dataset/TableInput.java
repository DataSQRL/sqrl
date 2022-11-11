package ai.datasqrl.io.sources.dataset;

import ai.datasqrl.io.formats.Format;
import ai.datasqrl.io.formats.FormatConfiguration;
import ai.datasqrl.io.sources.DataSystemConnector;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;

public class TableInput extends AbstractExternalTable {

    public TableInput(DataSystemConnector dataset, TableConfig configuration, NamePath path, Name name) {
        super(dataset, configuration, path, name);
    }

    public boolean hasSourceTimestamp() {
        return connector.hasSourceTimestamp();
    }

    public Format.Parser getParser() {
        FormatConfiguration format = configuration.getFormat();
        return format.getImplementation().getParser(format);
    }

}
