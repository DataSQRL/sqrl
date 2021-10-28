package ai.dataeng.sqml.logical3;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import java.util.Optional;

public class QueryRelationField extends StandardField {
  public QueryRelationField(Name name, RelationType<TypedField> type) {
    this(name, type, Optional.empty());
  }

  public QueryRelationField(Name name, RelationType<TypedField> type, Optional<String> alias) {
    super(name, type, List.of(), alias);
  }

  @Override
  public Field withAlias(String alias) {
    return new QueryRelationField(getName(), (RelationType)getType(), Optional.of(alias));
  }
}