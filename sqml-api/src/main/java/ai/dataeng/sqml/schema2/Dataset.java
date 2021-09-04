package ai.dataeng.sqml.schema2;

import ai.dataeng.sqml.schema2.name.Name;
import lombok.Value;

import java.util.Map;

@Value
public class Dataset<Context> {

    private final Name name;
    private final Map<Name,Field<RelationType,Context>> tables;

}
