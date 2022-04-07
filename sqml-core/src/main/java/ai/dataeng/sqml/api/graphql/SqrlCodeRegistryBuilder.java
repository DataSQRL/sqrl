package ai.dataeng.sqml.api.graphql;

import ai.dataeng.execution.DefaultDataFetcher;
import ai.dataeng.execution.SqlClientProvider;
import ai.dataeng.execution.criteria.Criteria;
import ai.dataeng.execution.page.SystemPageProvider;
import ai.dataeng.execution.table.H2Table;
import ai.dataeng.execution.table.TableFieldFetcher;
import ai.dataeng.execution.table.column.Columns;
import ai.dataeng.execution.table.column.DateTimeColumn;
import ai.dataeng.execution.table.column.FloatColumn;
import ai.dataeng.execution.table.column.H2Column;
import ai.dataeng.execution.table.column.IntegerColumn;
import ai.dataeng.execution.table.column.StringColumn;
import ai.dataeng.execution.table.column.UUIDColumn;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.planner.nodes.LogicalFlinkSink;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.type.basic.FloatType;
import ai.dataeng.sqml.type.basic.IntegerType;
import ai.dataeng.sqml.type.basic.StringType;
import ai.dataeng.sqml.type.basic.UuidType;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLCodeRegistry.Builder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.TableDescriptor;

public class SqrlCodeRegistryBuilder {
  GraphQLCodeRegistry.Builder registry = GraphQLCodeRegistry.newCodeRegistry();
  private Map<Table, TableDescriptor> physicalDescriptors;

  public GraphQLCodeRegistry build(SqlClientProvider sqlClientProvider,
      Map<Table, LogicalFlinkSink> sinks,
      Map<Table, TableDescriptor> physicalDescriptors) {
    this.physicalDescriptors = physicalDescriptors;

    for (Map.Entry<Table, LogicalFlinkSink> sink : sinks.entrySet()) {
      registerFetcher(registry, sqlClientProvider, sink.getKey(), sink.getValue());
    }

    return registry.build();
  }

  private void registerFetcher(Builder registry, SqlClientProvider pool, Table table, LogicalFlinkSink flinkSink) {
    NamePath path = table.getPath();

    if (path.getLength() == 1) {//register a special fetcher
      System.out.println("Registering df on :" + table.name.getDisplay() + " with :" + flinkSink.getPhysicalName());

      registry.dataFetcher(
          FieldCoordinates.coordinates("Query", table.name.getCanonical()),
          new DefaultDataFetcher(pool, new SystemPageProvider(), toTable(table, Optional.empty(), flinkSink.getPhysicalName())));
    }


    //Look for any relationship tables and build fetchers for them
//    for (Field field : table.getFields()) {
//      if (field instanceof Relationship) {
//        Relationship rel = (Relationship) field;
//        Criteria criteria = buildCriteria(rel);
//
//        System.out.println(getTypeName(table.getPath())+" : "+ field.getName().getCanonical());
//        registry.dataFetcher(
//            FieldCoordinates.coordinates(getTypeName(table.getPath()), field.getName().getCanonical()),
//            new DefaultDataFetcher(pool, new NoPage(), toTable(rel.getToTable(), Optional.of(criteria))));
//        registerFetcher(registry, pool, rel.getToTable(), seen);
//      }
//    }
  }

//  private Criteria buildCriteria(Relationship rel) {
//    Criteria criteria = new EqualsCriteria(rel.getFrom().get(0).getName().getCanonical(),
//        ((Column)rel.getTo().get(0)).getId());
//    return criteria;
//  }
//
//  private String getTypeName(NamePath path) {
//    return String.join(ID_DELIMITER,
//        Arrays.stream(path.getNames())
//            .map(e->e.getDisplay())
//            .collect(Collectors.toList()));
//  }
//
//  private Criteria buildCriteria(Relationship rel) {
//    Criteria criteria = new EqualsCriteria(rel.getFrom().get(0).getName().getCanonical(),
//        ((Column)rel.getTo().get(0)).getId());
//    return criteria;
//  }
//
//  private String getTypeName(NamePath path) {
//    return String.join(ID_DELIMITER,
//        Arrays.stream(path.getNames())
//            .map(e->e.getDisplay())
//            .collect(Collectors.toList()));
//  }

  private TableFieldFetcher toTable(Table table, Optional<Criteria> criteria, String tableName) {
    return new TableFieldFetcher(new H2Table(new Columns(toColumns(table)), tableName),
        criteria);
  }

  private List<H2Column> toColumns(Table table) {
    List<H2Column> list = new ArrayList<>();
    for (Field field : table.getFields()) {
      if (field instanceof Column) {
        list.add(toH2Column((Column)field));
      }
    }

    return list;
  }

  private H2Column toH2Column(Column column) {
    BasicType basicType = column.getType();

    return basicType.accept(new SqmlTypeVisitor<>(){
      @Override
      public H2Column visitIntegerType(IntegerType type, Column context) {
        return new IntegerColumn(context.getName().getDisplay(), context.getId().toString());
      }

      @Override
      public H2Column visitStringType(StringType type, Column context) {
        return new StringColumn(context.getName().getDisplay(), context.getId().toString());
      }

      @Override
      public H2Column visitUuidType(UuidType type, Column context) {
        return new UUIDColumn(context.getName().getDisplay(), context.getId().toString());
      }

      @Override
      public H2Column visitFloatType(FloatType type, Column context) {
        return new FloatColumn(context.getName().getDisplay(), context.getId().toString());
      }

      @Override
      public H2Column visitDateTimeType(DateTimeType type, Column context) {
        return new DateTimeColumn(context.getName().getDisplay(), context.getId().toString());
      }

      @Override
      public H2Column visitType(Type type, Column context) {
        throw new RuntimeException("Unknown type:" + type.getClass().getName());
      }
    }, column);
  }
}
