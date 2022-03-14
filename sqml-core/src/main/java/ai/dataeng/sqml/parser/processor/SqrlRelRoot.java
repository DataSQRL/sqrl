package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.planner.Column;
import java.util.Map;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;

@Value
public class SqrlRelRoot {

  private final Map<RelDataTypeField, Column> fieldList;
  private final RelNode node;

}
