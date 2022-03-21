package ai.dataeng.sqml.planner.macros;

import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.schema.SqrlCalciteTable;
import org.apache.calcite.sql.SpecialSqlFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlScopedShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

@AllArgsConstructor
public class ExtractToManyAggs extends SqlBasicVisitor {
  SqlValidator validator;
  SqlSelect select;

  final Multimap<SqrlCalciteTable, SqlCall> aggs = ArrayListMultimap.create();

  public static Multimap<SqrlCalciteTable, SqlCall> accept(SqlSelect select, SqlValidator validator) {
    ExtractToManyAggs extract = new ExtractToManyAggs(validator, select);
    select.accept(extract);
    return extract.aggs;
  }

  @Override
  public Object visit(SqlCall call) {
    if (!(call.getOperator() instanceof SpecialSqlFunction)) {
      return super.visit(call);
    }

    for (SqlNode node : call.getOperandList()) {
      ToManyIdentifierExtractor extractor = new ToManyIdentifierExtractor(validator.getSelectScope(select), aggs);
      node.accept(extractor);

      if (!extractor.getIdentifiers().isEmpty()) {
        for (Entry<SqlIdentifier, SqrlCalciteTable> entry : extractor.getIdentifiers().entrySet()) {
          aggs.put(entry.getValue(), call);
        }
      }
    }

    return super.visit(call);
  }

  class ToManyIdentifierExtractor extends SqlScopedShuttle {
    @Getter
    Map<SqlIdentifier, SqrlCalciteTable> identifiers = new HashMap<>();

    protected ToManyIdentifierExtractor(
        SqlValidatorScope initialScope,
        Multimap<SqrlCalciteTable, SqlCall> aggs) {
      super(initialScope);
    }

    @Override
    public SqlNode visit(SqlIdentifier identifier) {
      SqrlCalciteTable table = (SqrlCalciteTable) getScope().fullyQualify(identifier).namespace.getRowType();
      if (table == null) {
        return super.visit(identifier);
      }

      Optional<Field> toMany = table.getSqrlTable().getField(NamePath.parse(identifier.names.get(1)))
          .stream()
          .flatMap(f->f.getFields().stream())
          .filter(f->f instanceof Relationship)
          .filter(f->((Relationship)f).getMultiplicity() == Multiplicity.MANY)
          .findAny();
      if (toMany.isPresent()) {
        identifiers.put(identifier, table);
      }

      return null;
    }
  }


}
