package org.apache.calcite.rel.type;

import ai.datasqrl.schema.Relationship;
import java.util.List;
import java.util.stream.Collectors;

public class NestedRelDataTypeFieldImpl extends RelDataTypeFieldImpl {

  private final List<Relationship> rels;

  public NestedRelDataTypeFieldImpl(RelDataTypeField field, List<Relationship> rels) {
    super(toName(rels, field.getName()), field.getIndex(), field.getType());
    this.rels = rels;
  }

  private static String toName(List<Relationship> rels, String name) {
    String prefix = rels.stream().map(e->e.getName().getCanonical() + ".")
        .collect(Collectors.joining("."));
    return prefix + name;
  }
}
