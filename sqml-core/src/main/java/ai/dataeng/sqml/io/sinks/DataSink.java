package ai.dataeng.sqml.io.sinks;

import ai.dataeng.sqml.io.formats.Format;
import ai.dataeng.sqml.io.formats.FormatConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class DataSink {

    private final Name name;
    private final DataSinkConfiguration configuration;
    private final FormatConfiguration format;

    public DataSink(DataSinkRegistration reg) {
        name = Name.system(reg.getName());
        configuration = reg.getConfig();
        format = reg.getFormatConfig();
    }

    public Name getName() {
        return name;
    }

    public DataSinkConfiguration getConfiguration() {
        return configuration;
    }

    public DataSinkRegistration getRegistration() {
        return new DataSinkRegistration(name.getDisplay(),configuration,format.getName(),format);
    }

    public Format.Writer getWriter() {
        return format.getImplementation().getWriter(format);
    }

    public TableSink getTableSink(Name name) {
        return new TableSink(name,this);
    }

}
