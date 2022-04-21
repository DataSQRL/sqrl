package ai.datasqrl.schema.type.schema.external;

import java.io.Serializable;

public class AbstractElementDefinition implements Serializable {

  public String name;
  public String description;

  public Object default_value;
  //TODO: add hints


}
