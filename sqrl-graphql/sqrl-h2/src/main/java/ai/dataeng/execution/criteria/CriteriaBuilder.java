package ai.dataeng.execution.criteria;

import ai.dataeng.execution.criteria.CriteriaBuilder.CriteriaResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import lombok.Value;

@Value
public class CriteriaBuilder extends CriteriaVisitor<CriteriaResult, Object> {
  DataFetchingEnvironment environment;

  @Override
  public CriteriaResult visit(EqualsCriteria equalsCriteria, Object context) {
    Map<String, Object> source = environment.getSource();
    Object value = source.get(equalsCriteria.getEnvVar());

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
