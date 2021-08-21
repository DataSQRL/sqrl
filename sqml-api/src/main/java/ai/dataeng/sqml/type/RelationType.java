package ai.dataeng.sqml.type;

import ai.dataeng.sqml.analyzer.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RelationType extends Type {
  public static RelationType INSTANCE = new RelationType();

  protected List<Field> fields;
  protected Optional<RelationType> parent;

  public RelationType() {
    this(new ArrayList<>());
  }

  public RelationType(List<Field> fields) {
    super("RELATION");
    this.fields = fields;
    this.parent = Optional.empty();
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitRelation(this, context);
  }

  public List<Field> getFields() {
    return fields;
  }

  public List<Field> getVisibleFields() {
    return fields.stream()
        .filter(f->!f.isHidden())
        .collect(Collectors.toList());
  }

  public void addField(Field field) {
    //todo shadow fields
    for (int i = fields.size() - 1; i >= 0; i--) {
      Field f = fields.get(i);
      if (f.getName().get().equalsIgnoreCase(field.getName().get())) {
        fields.remove(i);
      }
    }
    this.fields.add(field);
  }

  public Optional<Field> getField(String name) {
    for (Field field : fields) {
      if (field.getName().isPresent() && field.getName().get().equals(name)) {
        return Optional.of(field);
      }
    }
    return Optional.empty();
  }

  public void setParent(RelationType parent) {
    this.parent = Optional.of(parent);
    addField(Field.newUnqualified("parent", parent, true));
  }

  public Optional<RelationType> getParent() {
    return parent;
  }

  public int indexOf(Field field) {
    for (int i = 0; i < fields.size(); i++) {
      if(fields.get(i) == field) {
        return i;
      }
    }
    return -1;
  }

  public static class NamedRelationType extends RelationType {
    private RelationType delegate;
    protected final String relationName;

    public NamedRelationType(RelationType delegate, String relationName) {
      this.delegate = delegate;
      this.relationName = relationName;
    }
    @Override
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitNamedRelation(this, context);
    }

    @Override
    public List<Field> getFields() {
      return delegate.getFields();
    }

    @Override
    public List<Field> getVisibleFields() {
      return delegate.getVisibleFields();
    }

    @Override
    public void setParent(RelationType parent) {
      delegate.setParent(parent);
    }

    @Override
    public Optional<RelationType> getParent() {
      return delegate.getParent();
    }

    @Override
    public void addField(Field field) {
      delegate.addField(field);
    }

    @Override
    public Optional<Field> getField(String name) {
      return delegate.getField(name);
    }

    public String getRelationName() {
      return relationName;
    }
  }
  public static class RootRelationType extends RelationType {
    private RelationType delegate;

    public RootRelationType(RelationType delegate) {
      this.delegate = delegate;
    }

    @Override
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitRootRelation(this, context);
    }

    @Override
    public List<Field> getVisibleFields() {
      return delegate.getVisibleFields();
    }

    @Override
    public void setParent(RelationType parent) {
      delegate.setParent(parent);
    }

    @Override
    public Optional<RelationType> getParent() {
      return delegate.getParent();
    }

    @Override
    public List<Field> getFields() {
      return delegate.getFields();
    }

    @Override
    public void addField(Field field) {
      delegate.addField(field);
    }

    @Override
    public Optional<Field> getField(String name) {
      return delegate.getField(name);
    }
  }
  public static class ImportRelationType extends NamedRelationType {
    public ImportRelationType(RelationType delegate, String name) {
      super(delegate, name);
    }

    @Override
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitImportRelation(this, context);
    }
  }
}
