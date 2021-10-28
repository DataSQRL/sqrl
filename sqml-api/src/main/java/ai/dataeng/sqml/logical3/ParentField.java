package ai.dataeng.sqml.logical3;

import static ai.dataeng.sqml.logical3.LogicalPlan.Builder.unbox;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import java.util.Optional;

/**
   * Field that resolve when resolving 'parent'
   */
public class ParentField implements TypedField {

  private final TypedField field;
  private final Optional<QualifiedName> alias;

  public ParentField(TypedField field) {
    this(field, Optional.empty());
  }

  public ParentField(TypedField field, Optional<QualifiedName> alias) {
    this.field = field;
    this.alias = alias;
  }

  @Override
  public Name getName() {
    return Name.of("parent", NameCanonicalizer.SYSTEM);
  }

  @Override
  public Type getType() {
    return field.getType();
  }

  @Override
  public boolean isHidden() {
    return true;
  }

  @Override
  public Optional<QualifiedName> getAlias() {
    return alias;
  }

  @Override
  public Field withAlias(QualifiedName alias) {
    return new ParentField(field, Optional.of(alias));
  }

  @Override
  public QualifiedName getQualifiedName() {
    Type type = unbox(field.getType());
    if (type instanceof RelationType) {
      Optional<Field> field = ((RelationType) type).getField("parent");
      if (field.isPresent() && field.get() instanceof TypedField) {
        QualifiedName parentName = ((TypedField)field.get().getType()).getQualifiedName();

        return parentName.append(this.field.getName());
      }
    }

    return QualifiedName.of(this.field.getName());
  }
}
