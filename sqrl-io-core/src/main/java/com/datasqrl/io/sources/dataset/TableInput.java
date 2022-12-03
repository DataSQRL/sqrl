package com.datasqrl.io.sources.dataset;

import com.datasqrl.io.formats.Format;
import com.datasqrl.io.formats.FormatConfiguration;
import com.datasqrl.io.sources.DataSystemConnector;
import com.datasqrl.parse.tree.name.Name;
import com.datasqrl.parse.tree.name.NamePath;

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
