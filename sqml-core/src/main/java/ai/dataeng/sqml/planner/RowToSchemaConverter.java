package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.basic.BooleanType;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.type.basic.FloatType;
import ai.dataeng.sqml.type.basic.IntegerType;
import ai.dataeng.sqml.type.basic.IntervalType;
import ai.dataeng.sqml.type.basic.NullType;
import ai.dataeng.sqml.type.basic.StringType;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;

public class RowToSchemaConverter {
  public static List<Column> convert(RelNode relNode) {
    RowToSchemaConverter converter = new RowToSchemaConverter();
    return converter.convertRelDatatype(relNode);
  }

  @Value
  static class Grouping {
    List<Integer> keyColumns;
  }
//
//  private static Grouping extractKeyColumns(RelNode node) {
//    if (node instanceof TableScan) {
//      /*Must be the _ or */
//      SqrlLogicalTableScan tableScan = (SqrlLogicalTableScan) node;
//      List<Integer> keyColumns = RelColumnTranslator.getColumnIndices(
//          tableScan.getSqrlTable().getPrimaryKeys(),
//          node.getRowType());
//      Preconditions.checkState(!keyColumns.isEmpty());
//
//      return new Grouping(keyColumns);
//    } else if (node instanceof Aggregate) {
//      Aggregate agg = (Aggregate) node;
//
//      ImmutableBitSet groupSet = agg.getGroupSet();
//      List<Integer> groups = groupSet.asList();
//
//      return new Grouping(groups);
//    } else if (node instanceof Project) {
//      Project project = (Project) node;
//      Grouping grouping = extractKeyColumns(project.getInput());
//
//    } else if (node instanceof Sort) {
//
//    } else if (node instanceof Join) {
//
//    } else if (node instanceof LogicalFilter) {
//
//    }

//    throw new RuntimeException("");
//
//
//  }

  private List<Column> convertRelDatatype(RelNode relNode) {
    return convertRelDatatype(relNode.getRowType());
  }

  /**
   * C
   * @param row
   * @return
   */
  public List<Column> convertRelDatatype(RelDataType row) {
    List<Column> fields = row.getFieldList().stream()
        .map(this::toField)
        .collect(Collectors.toList());
    
    return fields;
  }

  private Column toField(RelDataTypeField relDataTypeField) {
    return new Column(Name.system(relDataTypeField.getName()), null, 0,
        toBasicType(relDataTypeField.getType()), 0, List.of(), 
        false, false, false);
  }

  private BasicType toBasicType(RelDataType type) {
    if (!(type instanceof BasicSqlType)) {
      throw new RuntimeException("Unknown column:" + type.getClass().getName());
    }
    
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return BooleanType.INSTANCE;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return IntegerType.INSTANCE;
      case DECIMAL:
      case FLOAT:
      case REAL:
      case DOUBLE:
        return FloatType.INSTANCE;
      case DATE:
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return DateTimeType.INSTANCE;
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        return IntervalType.INSTANCE;
      case CHAR:
      case VARCHAR:
        return StringType.INSTANCE;
      case NULL:
        return NullType.INSTANCE;
      case BINARY:
      case VARBINARY:
      case ANY:
      case SYMBOL:
      case MULTISET:
      case ARRAY:
      case MAP:
      case DISTINCT:
      case STRUCTURED:
      case ROW:
      case OTHER:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case GEOMETRY:
      case SARG:
        throw new RuntimeException("unknown type");
    }
    
    throw new RuntimeException("Unknown column:" + type) ;
  }
}
