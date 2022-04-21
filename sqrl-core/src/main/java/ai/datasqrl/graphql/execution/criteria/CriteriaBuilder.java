package ai.datasqrl.graphql.execution.criteria;

import ai.datasqrl.graphql.execution.criteria.CriteriaBuilder.CriteriaResult;
import com.google.common.base.Preconditions;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import lombok.Value;

@Value
public class CriteriaBuilder extends CriteriaVisitor<CriteriaResult, Object> {

  DataFetchingEnvironment environment;

  @Override
  public CriteriaResult visit(EqualsCriteria equalsCriteria, Object context) {
    Object source = environment.getSource();

    Preconditions.checkState(source instanceof Map, "Only map types expected");
    Map<String, Object> sourceMap = (Map<String, Object>) source;
    Object value = sourceMap.get(equalsCriteria.getEnvVar());
    Preconditions.checkNotNull(value, "Missing criteria column. Looking for %s, found %s",
        equalsCriteria.getEnvVar(), sourceMap);

    CriteriaResult result = new CriteriaResult(
        List.of(String.format(" %s = ? ", equalsCriteria.getColumnName())),
        List.of(value)
    );

    return result;
  }

  @Value
  public class CriteriaResult {

    List<String> clauseList;
    List<Object> valuesList;
  }
}
