package ai.dataeng.sqml.io.sources;

import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DataSourceUpdate {

    String name;

    @NonNull @NotNull @Valid
    DataSourceConfiguration config;

    /**
     * Whether this datasource should automatically discover available tables
     * when the data source is added and register those tables with the source.
     *
     * If false, tables have to be added explicitly through the configuration.
     */
    @Builder.Default
    boolean discoverTables = true;

    @Valid @Builder.Default @NonNull @NotNull
    List<SourceTableConfiguration> tables = Collections.EMPTY_LIST;

}
