package ai.dataeng.sqml.ingest.schema;

import com.google.common.base.Strings;
import lombok.Value;

@Value
public class SchemaElementDescription {

    public static final SchemaElementDescription NONE = new SchemaElementDescription("");

    private final String description;


    public boolean isEmpty() {
        return Strings.isNullOrEmpty(description);
    }

    public static SchemaElementDescription of(String description) {
        if (Strings.isNullOrEmpty(description)) return NONE;
        else return new SchemaElementDescription(description);
    }

}
