package ai.datasqrl.schema;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;

//used for type discovery
public class RelTypeWithHidden extends RelRecordType {

  private final RelDataType hidden;

  public RelTypeWithHidden(RelDataType hidden, List<RelDataTypeField> fieldList) {
    super(StructKind.PEEK_FIELDS_NO_EXPAND, hidden.getFieldList());
    this.hidden = hidden;
  }

//  @Override
//  public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
//    return fi
//  }
}