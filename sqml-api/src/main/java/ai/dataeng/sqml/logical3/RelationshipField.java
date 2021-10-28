package ai.dataeng.sqml.logical3;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.name.Name;
import java.util.Optional;
import lombok.Getter;

@Getter
public class RelationshipField implements TypedField {

  private final Name name;
  private final TypedField to;
  private final Optional<QualifiedName> alias;

  //private final the-actual-join-definition from the AST plus identifier dereferencing

  public RelationshipField(Name name, TypedField to) {
    this(name, to, Optional.empty());
  }

  public RelationshipField(Name name, TypedField to, Optional<QualifiedName> alias) {
    this.name = name;
    this.to = to;
    this.alias = alias;
  }

  @Override
  public Type getType() {
    return to.getType();
  }

  @Override
  public Field withAlias(QualifiedName alias) {
    return new RelationshipField(getName(), to, Optional.of(alias));
  }
}
