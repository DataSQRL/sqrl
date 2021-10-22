package ai.dataeng.sqml.ingest.schema;

import com.google.common.base.Strings;
import lombok.*;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class SchemaElementDescription implements Serializable {

    public static final SchemaElementDescription NONE = new SchemaElementDescription("");

    private String description;


    public boolean isEmpty() {
        return Strings.isNullOrEmpty(description);
    }

    public static SchemaElementDescription of(String description) {
        if (Strings.isNullOrEmpty(description)) return NONE;
        else return new SchemaElementDescription(description);
    }

}
