//package ai.datasqrl.graphql;
//
//import ai.datasqrl.graphql.execution.DefaultDataFetcher;
//import ai.datasqrl.graphql.execution.SqlClientProvider;
//import ai.datasqrl.graphql.execution.criteria.Criteria;
//import ai.datasqrl.graphql.execution.criteria.EqualsCriteria;
//import ai.datasqrl.graphql.execution.page.NoPage;
//import ai.datasqrl.graphql.execution.page.SystemPageProvider;
//import ai.datasqrl.graphql.execution.table.H2Table;
//import ai.datasqrl.graphql.execution.table.TableFieldFetcher;
//import ai.datasqrl.graphql.execution.table.column.BooleanColumn;
//import ai.datasqrl.graphql.execution.table.column.Columns;
//import ai.datasqrl.graphql.execution.table.column.DateTimeColumn;
//import ai.datasqrl.graphql.execution.table.column.FloatColumn;
//import ai.datasqrl.graphql.execution.table.column.H2Column;
//import ai.datasqrl.graphql.execution.table.column.IntegerColumn;
//import ai.datasqrl.graphql.execution.table.column.StringColumn;
//import ai.datasqrl.graphql.execution.table.column.UUIDColumn;
//import ai.datasqrl.schema.Column;
//import ai.datasqrl.schema.Field;
//import ai.datasqrl.schema.Relationship;
//import ai.datasqrl.schema.Table;
//import ai.datasqrl.plan.nodes.LogicalFlinkSink;
//import ai.datasqrl.parse.tree.name.NamePath;
//import ai.datasqrl.schema.type.SqmlTypeVisitor;
//import ai.datasqrl.schema.type.Type;
//import ai.datasqrl.schema.type.basic.BasicType;
//import ai.datasqrl.schema.type.basic.BigIntegerType;
//import ai.datasqrl.schema.type.basic.BooleanType;
//import ai.datasqrl.schema.type.basic.DateTimeType;
//import ai.datasqrl.schema.type.basic.DoubleType;
//import ai.datasqrl.schema.type.basic.FloatType;
//import ai.datasqrl.schema.type.basic.IntegerType;
//import ai.datasqrl.schema.type.basic.StringType;
//import ai.datasqrl.schema.type.basic.UuidType;
//import graphql.schema.FieldCoordinates;
//import graphql.schema.GraphQLCodeRegistry;
//import graphql.schema.GraphQLCodeRegistry.Builder;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.stream.Collectors;
//import org.apache.flink.table.api.TableDescriptor;
//
//public class SqrlCodeRegistryBuilder {
//  GraphQLCodeRegistry.Builder registry = GraphQLCodeRegistry.newCodeRegistry();
//  private Map<Table, TableDescriptor> physicalDescriptors;
//
//  public GraphQLCodeRegistry build(SqlClientProvider sqlClientProvider,
//      Map<Table, LogicalFlinkSink> sinks,
//      Map<Table, TableDescriptor> physicalDescriptors) {
//    this.physicalDescriptors = physicalDescriptors;
//
//    for (Map.Entry<Table, LogicalFlinkSink> sink : sinks.entrySet()) {
//      registerFetcher(registry, sqlClientProvider, sink.getKey(), sink.getValue());
//    }
//
//    return registry.build();
//  }
//
//  private void registerFetcher(Builder registry, SqlClientProvider pool, Table table, LogicalFlinkSink flinkSink) {
//    NamePath path = table.getPath();
//
//    if (path.getLength() == 1) {//register a special fetcher
//      System.out.println("Registering df on :" + table.name.getDisplay() + " with :" + flinkSink.getPhysicalName());
//
//      registry.dataFetcher(
//          FieldCoordinates.coordinates("Query", table.name.getCanonical()),
//          new DefaultDataFetcher(pool, new SystemPageProvider(), toTable(table, Optional.empty(), flinkSink.getPhysicalName())));
//    }
//
//    //Look for any relationship tables and build fetchers for them
//    for (Field field : table.getFields()) {
//      if (field instanceof Relationship) {
//        Relationship rel = (Relationship) field;
//        Criteria criteria = buildCriteria(rel);
//
//        System.out.println(getTypeName(table.getPath())+" : "+ field.getName().getCanonical());
//        registry.dataFetcher(
//            FieldCoordinates.coordinates(getTypeName(table.getPath()), field.getName().getCanonical()),
//            new DefaultDataFetcher(pool, new NoPage(), toTable(rel.getToTable(), Optional.of(criteria), rel.getToTable().getName()
//                .getDisplay().toString())));
//
//        //todo: more than one level deep
//        //registerFetcher(registry, pool, rel.getToTable(), seen);
//      }
//    }
//  }
//
//  private Criteria buildCriteria(Relationship rel) {
//    Criteria criteria = new EqualsCriteria(rel.getTable().getPrimaryKeys().get(0).getId().toString(),
//        rel.getTable().getPrimaryKeys().get(0).getId().toString());
//    return criteria;
//  }
////
//  private String getTypeName(NamePath path) {
//    return String.join("_",
//        Arrays.stream(path.getNames())
//            .map(e->e.getDisplay())
//            .collect(Collectors.toList()));
//  }
//
//  private TableFieldFetcher toTable(Table table, Optional<Criteria> criteria, String tableName) {
//    return new TableFieldFetcher(new H2Table(new Columns(toColumns(table)), tableName),
//        criteria);
//  }
//
//  private List<H2Column> toColumns(Table table) {
//    List<H2Column> list = new ArrayList<>();
//    for (Field field : table.getFields()) {
//      if (field instanceof Column) {
//        list.add(toH2Column((Column)field));
//      }
//    }
//
//    return list;
//  }
//
//  private H2Column toH2Column(Column column) {
//    BasicType basicType = column.getType();
//
//    return basicType.accept(new SqmlTypeVisitor<>(){
//      @Override
//      public H2Column visitIntegerType(IntegerType type, Column context) {
//        return new IntegerColumn(context.getName().getDisplay(), context.getId().toString());
//      }
//      @Override
//      public H2Column visitDoubleType(DoubleType type, Column context) {
//        return new FloatColumn(context.getName().getDisplay(), context.getId().toString());
//      }
//
//      @Override
//      public H2Column visitStringType(StringType type, Column context) {
//        return new StringColumn(context.getName().getDisplay(), context.getId().toString());
//      }
//
//      @Override
//      public H2Column visitUuidType(UuidType type, Column context) {
//        return new UUIDColumn(context.getName().getDisplay(), context.getId().toString());
//      }
//
//      @Override
//      public H2Column visitFloatType(FloatType type, Column context) {
//        return new FloatColumn(context.getName().getDisplay(), context.getId().toString());
//      }
//
//      @Override
//      public H2Column visitDateTimeType(DateTimeType type, Column context) {
//        return new DateTimeColumn(context.getName().getDisplay(), context.getId().toString());
//      }
//
//      @Override
//      public H2Column visitBooleanType(BooleanType type, Column context) {
//        return new BooleanColumn(context.getName().getDisplay(), context.getId().toString());
//      }
//
//      @Override
//      public H2Column visitBigIntegerType(BigIntegerType type, Column context) {
//        return new IntegerColumn(context.getName().getDisplay(), context.getId().toString());
//      }
//
//      @Override
//      public H2Column visitType(Type type, Column context) {
//        throw new RuntimeException("Unknown type:" + type.getClass().getName());
//      }
//    }, column);
//  }
//}
