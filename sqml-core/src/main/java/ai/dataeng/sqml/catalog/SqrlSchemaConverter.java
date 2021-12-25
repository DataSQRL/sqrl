package ai.dataeng.sqml.catalog;

import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.LogicalPlanImpl.Relationship;
import ai.dataeng.sqml.planner.LogicalPlanImpl.Relationship.Multiplicity;
import ai.dataeng.sqml.planner.LogicalPlanImpl.Relationship.Type;
import ai.dataeng.sqml.planner.LogicalPlanImpl.Table;
import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.basic.BigIntegerType;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.type.basic.IntegerType;
import ai.dataeng.sqml.type.basic.StringType;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

public class SqrlSchemaConverter {

  public LogicalPlanImpl convert(SqrlCatalogManager tableManager) {
    LogicalPlanImpl logicalPlan = new LogicalPlanImpl();
//
//    Map<SqrlTable, Table> entityTableMap = new HashMap<>();
//    for (SqrlTable entry : tableManager.getCurrentTables()) {
//      Table table = new Table(logicalPlan.getTableIdCounter().incrementAndGet(),
//          entry.getNamePath().getLast(), false);
//      populateTable(table, entry);
//      entityTableMap.put(entry, table);
//    }
//
//    for (Map.Entry<SqrlTable, Table> tbl : entityTableMap.entrySet()) {
//      SqrlTable entity = tbl.getKey();
//      Table table = tbl.getValue();
//      if (entity.getNamePath().getLength() == 1) {
//        logicalPlan.getSchema().add(table);
//      }
//      populateRelationships(entity, table, entityTableMap);
//
//    }



    System.out.println(logicalPlan.getSchema());
    return logicalPlan;
  }

  private void populateRelationships(SqrlTable entity, Table table,
      Map<SqrlTable, Table> entityTableMap) {

    for (Map.Entry<Name, SqrlTable> relationship : entity.getRelationships().entrySet()) {
      Table rel = entityTableMap.get(relationship.getValue());
      Preconditions.checkNotNull(rel, "Relationship table could not be found");
      table.addField(new Relationship(relationship.getKey(), table, rel, Type.CHILD, Multiplicity.MANY));
    }

  }

  private void populateTable(Table table, SqrlTable entity) {
    for (Column column : entity.getTable().getResolvedSchema().getColumns()) {
      table.addField(new LogicalPlanImpl.Column(
          Name.of(column.getName(), NameCanonicalizer.LOWERCASE_ENGLISH),
          table,
          0,
          column.getDataType().getLogicalType().accept(new ToBasicType()),
          0,
          List.of(),
          false,
          false
      ));
    }
  }
  public class ToBasicType implements LogicalTypeVisitor<BasicType> {
    @Override
    public BasicType visit(CharType charType) {
      return StringType.INSTANCE;
    }

    @Override
    public BasicType visit(VarCharType varCharType) {
      return StringType.INSTANCE;
    }

    @Override
    public BasicType visit(BooleanType booleanType) {
      return ai.dataeng.sqml.type.basic.BooleanType.INSTANCE;
    }

    @Override
    public BasicType visit(BinaryType binaryType) {
      return null;
    }

    @Override
    public BasicType visit(VarBinaryType varBinaryType) {
      return null;
    }

    @Override
    public BasicType visit(DecimalType decimalType) {
      return ai.dataeng.sqml.type.basic.FloatType.INSTANCE;
    }

    @Override
    public BasicType visit(TinyIntType tinyIntType) {
      return IntegerType.INSTANCE;
    }

    @Override
    public BasicType visit(SmallIntType smallIntType) {
      return IntegerType.INSTANCE;
    }

    @Override
    public BasicType visit(IntType intType) {
      return IntegerType.INSTANCE;
    }

    @Override
    public BasicType visit(BigIntType bigIntType) {
      return BigIntegerType.INSTANCE;
    }

    @Override
    public BasicType visit(FloatType floatType) {
      return ai.dataeng.sqml.type.basic.FloatType.INSTANCE;
    }

    @Override
    public BasicType visit(DoubleType doubleType) {
      return ai.dataeng.sqml.type.basic.FloatType.INSTANCE;
    }

    @Override
    public BasicType visit(DateType dateType) {
      return DateTimeType.INSTANCE;
    }

    @Override
    public BasicType visit(TimeType timeType) {
      return DateTimeType.INSTANCE;
    }

    @Override
    public BasicType visit(TimestampType timestampType) {
      return DateTimeType.INSTANCE;
    }

    @Override
    public BasicType visit(ZonedTimestampType zonedTimestampType) {
      return DateTimeType.INSTANCE;
    }

    @Override
    public BasicType visit(LocalZonedTimestampType localZonedTimestampType) {
      return DateTimeType.INSTANCE;
    }

    @Override
    public BasicType visit(YearMonthIntervalType yearMonthIntervalType) {
      return null;
    }

    @Override
    public BasicType visit(DayTimeIntervalType dayTimeIntervalType) {
      return null;
    }

    @Override
    public BasicType visit(ArrayType arrayType) {
      return null;
    }

    @Override
    public BasicType visit(MultisetType multisetType) {
      return null;
    }

    @Override
    public BasicType visit(MapType mapType) {
      return null;
    }

    @Override
    public BasicType visit(RowType rowType) {
      return null;
    }

    @Override
    public BasicType visit(DistinctType distinctType) {
      return null;
    }

    @Override
    public BasicType visit(StructuredType structuredType) {
      return null;
    }

    @Override
    public BasicType visit(NullType nullType) {
      return null;
    }

    @Override
    public BasicType visit(RawType<?> rawType) {
      return null;
    }

    @Override
    public BasicType visit(SymbolType<?> symbolType) {
      return null;
    }

    @Override
    public BasicType visit(LogicalType logicalType) {
      return null;
    }
  }
    
}
