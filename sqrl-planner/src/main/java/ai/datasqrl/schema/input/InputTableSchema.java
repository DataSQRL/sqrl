package ai.datasqrl.schema.input;

import ai.datasqrl.schema.builder.UniversalTableBuilder;
import lombok.Value;

import java.io.Serializable;

@Value
public class InputTableSchema implements Serializable {

    private final FlexibleDatasetSchema.TableField schema;
    private final boolean hasSourceTimestamp;

    public UniversalTableBuilder getUniversalTableBuilder() {
        FlexibleTableConverter converter = new FlexibleTableConverter(this);
        FlexibleTable2UTBConverter utbConverter = new FlexibleTable2UTBConverter();
        return converter.apply(utbConverter);
    }

}
