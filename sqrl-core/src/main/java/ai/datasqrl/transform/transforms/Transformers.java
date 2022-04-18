package ai.datasqrl.transform.transforms;

import lombok.Value;

@Value
public class Transformers {
  public static final AddColumnToQuery addColumnToQuery = new AddColumnToQuery();
  public static final AddContextToQuery addContextToQuery = new AddContextToQuery();
  public static final ConvertLimitToWindow convertLimitToWindow = new ConvertLimitToWindow();
  public static final AliasFirstColumn aliasFirstColumn = new AliasFirstColumn();
}
