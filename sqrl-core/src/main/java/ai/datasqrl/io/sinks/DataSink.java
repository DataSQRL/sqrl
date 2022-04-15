package ai.datasqrl.io.sinks;

import ai.datasqrl.io.formats.Format;
import ai.datasqrl.io.formats.FormatConfiguration;
import ai.datasqrl.parse.tree.name.Name;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class DataSink {

    private final Name name;
    private final DataSinkImplementation implementation;
    private final DataSinkConfiguration configuration;

    public DataSink(DataSinkRegistration reg) {
        name = Name.system(reg.getName());
        implementation = reg.getSink();
        configuration = reg.getConfig();
    }

    public Name getName() {
        return name;
    }

    public DataSinkImplementation getImplementation() {
        return implementation;
    }

    public DataSinkRegistration getRegistration() {
        return new DataSinkRegistration(name.getDisplay(), implementation, configuration);
    }

    public Format.Writer getWriter() {
        FormatConfiguration formatConfig = configuration.getFormat();
        return formatConfig.getImplementation().getWriter(formatConfig);
    }

    public TableSink getTableSink(Name name) {
        return new TableSink(name,this);
    }

}
