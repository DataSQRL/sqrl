package ai.dataeng.sqml.io.sinks;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.tree.name.Name;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@ToString
public class DataSinkRegistration {

    @NonNull @NotNull @Size(min=3)
    String name;

    @NonNull @NotNull @Valid
    DataSinkImplementation sink;

    @NonNull @NotNull @Valid @Builder.Default
    DataSinkConfiguration config = new DataSinkConfiguration();

    public boolean initialize(ErrorCollector errors) {
        if (!ConfigurationUtil.javaxValidate(this, errors)) return false;
        if (!Name.validName(name)) {
            errors.fatal("Sink needs to have valid name: %s", name);
            return false;
        }
        errors = errors.resolve(name);
        if (!sink.initialize(errors)) return false;
        if (!config.initialize(errors)) return false;
        return true;
    }

}
