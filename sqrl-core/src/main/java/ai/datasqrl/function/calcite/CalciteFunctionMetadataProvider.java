package ai.datasqrl.function.calcite;

import ai.datasqrl.function.FunctionMetadataProvider;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Note:
 * Calcite supports multiple function definitions with the same name and operators
 * can be determined based on their arguments. (e.g 'plus' having two conditions: x + y, +1.20).
 * However, we don't need to do this since we use the parser to determine the unary case.
 */
public class CalciteFunctionMetadataProvider implements FunctionMetadataProvider {
  private final Map<Name, SqrlAwareFunction> metadataMap;

  public CalciteFunctionMetadataProvider(SqlOperatorTable operatorTable) {
    metadataMap = operatorTable.getOperatorList()
        .stream()
        //TODO: Remove this once we have a standard library for functions
        //  It removes duplicate function names
        .filter(f-> !(
            f == SqlStdOperatorTable.UNARY_PLUS
            || f == SqlStdOperatorTable.DATETIME_PLUS
            || f == SqlStdOperatorTable.MINUS_DATE
            || f == SqlStdOperatorTable.UNARY_MINUS
            || f == SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR
            || f == SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR
            || f == SqlStdOperatorTable.MULTISET_QUERY
            || f == SqlStdOperatorTable.EXPLICIT_TABLE
            || f.getName().equalsIgnoreCase("")
        ))
        .map(f -> f instanceof SqrlAwareFunction ? (SqrlAwareFunction) f : new CalciteFunctionProxy(f))
        .distinct()
        .collect(Collectors.toMap(SqrlAwareFunction::getSqrlName, f->f));
  }

  @Override
  public Optional<SqrlAwareFunction> lookup(NamePath path) {
    return Optional.ofNullable(metadataMap.get(path.getLast()));
  }
}
