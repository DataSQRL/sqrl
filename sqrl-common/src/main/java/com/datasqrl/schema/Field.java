package com.datasqrl.schema;

import com.datasqrl.parse.tree.name.Name;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.sql.TableFunctionArgument;

@Getter
//@EqualsAndHashCode do not use
public abstract class Field {

  @NonNull
  protected final Name name;
  protected final int version;
  protected Optional<List<TableFunctionArgument>> tableArguments = Optional.empty();

  protected Field(@NonNull Name name, int version) {
    Preconditions.checkArgument(version>=0);
    this.name = name;
    this.version = version;
  }

  public Name getId() {
    if (version==0) return name;
    return name.suffix(Integer.toString(version));
  }

  public boolean isVisible() {
    return true;
  }

  @Override
  public String toString() {
    return getId().toString();
  }

  public abstract FieldKind getKind();
}