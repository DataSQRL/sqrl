package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.parser.Relationship.Multiplicity;
import ai.dataeng.sqml.parser.Relationship.Type;
import ai.dataeng.sqml.parser.macros.FieldFactory;
import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.JoinDeclaration;
import ai.dataeng.sqml.tree.name.VersionedName;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.sql.SqlSelect;

@Value
public class SqlNodeToFieldMapper {
  Namespace namespace;

  public List<Field> mapDistinct(ParseResult result) {
    List<Field> fields = FieldFactory.createFields(result.getValidator(),
        ((SqlSelect)result.getSqlNode()).getSelectList(),
        ((SqlSelect)result.getSqlNode()).getGroup(),
        Optional.of(result.getPrimaryKeyHint()));

    List<Field> resultFields = new ArrayList<>();
    for (Field field : fields) {
      if (field.getName().getCanonical().equals("__row_number")) {
        continue;
      }
      if (result.getPrimaryKeyHint().contains(field.getName().getCanonical().split(VersionedName.ID_DELIMITER_REGEX)[0])) {
        ((Column) field).setPrimaryKey(true);
      }
      resultFields.add(field);
    }

    return resultFields;
  }

  public Field mapExpression(ExpressionAssignment assignment, ParseResult result) {
    List<Field> fields = FieldFactory.createFields(result.getValidator(),
        ((SqlSelect)result.getSqlNode()).getSelectList(),  ((SqlSelect)result.getSqlNode()).getGroup(),
        Optional.empty());

    Optional<Field> field = fields.stream()
        .filter(f->f.getName().getCanonical().equals(assignment.getNamePath().getLast().getCanonical()))
        .findFirst();
    if (field.isEmpty()) {
      throw new RuntimeException("Internal error: Could not find field in expression");
    }
    Field f = field.get();

    return f;
  }

  public Relationship mapJoin(JoinDeclaration assignment, ParseResult result) {
    Table destination =  result.getDestinationTable().get();

    Multiplicity multiplicity = Multiplicity.MANY;
    //TODO:
//    if (assignment.getInlineJoin().getLimit().isPresent() &&
//        assignment.getInlineJoin().getLimit().get() == 1) {
//      multiplicity = Multiplicity.ONE;
//    }

    Table table = namespace.lookup(assignment.getNamePath().getPrefix().get()).get();
    Relationship rel = new Relationship(assignment.getNamePath().getLast(),
        table, destination, Type.JOIN, multiplicity, result.getAliases());

    rel.setSqlNode(result.getSqlNode());
    return rel;
  }

  public List<Field> mapQuery(ParseResult result) {
    List<Field> fields = FieldFactory.createFields(result.getValidator(),
        ((SqlSelect)result.getSqlNode()).getSelectList(),  ((SqlSelect)result.getSqlNode()).getGroup(),
        Optional.empty());
    //todo: internal fields
    return fields;
  }
}
