package ai.dataeng.sqml.schema2;


import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.name.Name;
import java.util.Optional;

public interface TypedField extends Field {

  public Type getType();

  default Optional<QualifiedName> canResolvePrefix(QualifiedName name) {
    if (this.getName() == null) {
      return Optional.empty();
    }

    if (getAlias().isPresent()) {
      QualifiedName alias = getAlias().get();
      QualifiedName prefix = alias.append(getName());
      if (name.hasPrefix(prefix)) {
        return Optional.of(prefix);
      }
    }

    if (name.hasPrefix(QualifiedName.of(getName()))) {
      return Optional.of(QualifiedName.of(getName()));
    }

    return Optional.empty();
  }
}
