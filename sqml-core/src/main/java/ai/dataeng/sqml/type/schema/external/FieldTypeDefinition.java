package ai.dataeng.sqml.type.schema.external;

import java.util.List;

public interface FieldTypeDefinition {

    String getType();

    List<FieldDefinition> getColumns();

    List<String> getTests();


}
