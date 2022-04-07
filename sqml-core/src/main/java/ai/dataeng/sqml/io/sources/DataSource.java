package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@ToString
public class DataSource implements Serializable {

    String name;
    DataSourceImplementation implementation;
    DataSourceConfiguration config;

    public DataSource(DataSourceUpdate update) {
        this(update.getName(),update.getSource(), update.getConfig());
    }

    public NameCanonicalizer getCanonicalizer() {
        return config.getNameCanonicalizer();
    }

    public Name getName() {
        return Name.system(name);
    }

    public DataSourceImplementation getImplementation() {
        return implementation;
    }

    public DataSourceConfiguration getConfig() {
        return config;
    }
}
