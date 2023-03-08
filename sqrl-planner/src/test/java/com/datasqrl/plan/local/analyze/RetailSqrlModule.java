package com.datasqrl.plan.local.analyze;

import static com.datasqrl.name.Name.HIDDEN_PREFIX;

import com.datasqrl.config.SourceFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.InMemSourceFactory;
import com.datasqrl.io.formats.CSVFormat;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.formats.JsonLineFormat.Configuration;
import com.datasqrl.io.impl.CanonicalizerConfiguration;
import com.datasqrl.io.impl.inmem.InMemConnector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.loaders.SqrlModule;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.calcite.TypeFactory;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.UniversalTable.Column;
import com.datasqrl.schema.UniversalTable.ImportFactory;
import com.datasqrl.schema.constraint.NotNull;
import com.datasqrl.schema.input.FlexibleFieldSchema.Field;
import com.datasqrl.schema.input.FlexibleFieldSchema.FieldType;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.RelationType;
import com.datasqrl.schema.input.SchemaElementDescription;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BooleanType;
import com.datasqrl.schema.type.basic.DateTimeType;
import com.datasqrl.schema.type.basic.FloatType;
import com.datasqrl.schema.type.basic.IntegerType;
import com.datasqrl.schema.type.basic.StringType;
import com.datasqrl.schema.type.basic.UuidType;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

public class RetailSqrlModule implements SqrlModule {

  private final Map<Name, NamespaceObject> tables;

  public RetailSqrlModule() {
    NamespaceObject customer = new TableSourceNamespaceObject(customerTableSource());
    NamespaceObject orders = new TableSourceNamespaceObject(orderTableSource());
    NamespaceObject product = new TableSourceNamespaceObject(productTableSource());

    this.tables = Stream.of(customer, orders, product)
        .collect(Collectors.toMap(e -> e.getName(), e -> e));
  }

  private TableSource orderTableSource() {

    RelDataTypeFactory typeFactory = TypeFactory.getTypeFactory();

    Function<Boolean, UniversalTable> createTable = (source) -> {
      ImportFactory importFactory =
          new UniversalTable.ImportFactory(typeFactory, true, source);

      UniversalTable table = importFactory.createTable(Name.system("orders"), NamePath.of("orders"),
          source);

      UniversalTable entries = importFactory.createTable(Name.system("entries"),
          NamePath.of("entries"), table, false);
      entries.addColumn(Name.system("productid"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
              false));
      entries.addColumn(Name.system("quantity"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
              false));
      entries.addColumn(Name.system("unit_price"), typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 5), false));
      entries.addColumn(Name.system("discount"), typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 5), true));

      table.addColumn(Name.system("id"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
              false));
      table.addColumn(Name.system("customerid"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
              false));
      table.addColumn(Name.system("time"), typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3), false));
      table.addChild(Name.system("entries"), entries, Multiplicity.MANY);
      return table;
    };

    TableConfig tableConfig = new TableConfig(ExternalDataType.source,
        CanonicalizerConfiguration.system,
        "UTF-8", new JsonLineFormat.Configuration(),
        "orders", "orders", "fixed",
        new InMemConnector("in-mem-orders", getOrderData()));

    return tableConfig.initializeSource(ErrorCollector.root(),
        NamePath.of("ecommerce-data"),
        createFlexibleSchema(createTable.apply(false)));
  }

  private TableSchema createFlexibleSchema(UniversalTable table) {
    return new FlexibleTableSchema(
        table.getName(), new SchemaElementDescription(""), null, false,
        createFields(table),
        List.of()
    );
  }

  private RelationType<Field> createFields(UniversalTable table) {
    return createFields(table.getFields().getFields());
  }

  private RelationType<Field> createFields(List<com.datasqrl.schema.Field> f) {
    List<Field> fields = new ArrayList<>();
    for (com.datasqrl.schema.Field field : f) {
      if (field.getName().getCanonical().startsWith(
          HIDDEN_PREFIX)) { //todo: what if a user defines a _ingest_time in the schema.yml?
        continue;
      }
      if (field instanceof UniversalTable.Column) {
        UniversalTable.Column column = (UniversalTable.Column) field;
        fields.add(new Field(
            field.getName(),
            new SchemaElementDescription(""),
            null,
            List.of(new FieldType(
                column.getName(),
                toType(column), //other types
                0,
                column.isNullable() ? List.of() : List.of(NotNull.INSTANCE) //not null
            ))
        ));
      } else if (field instanceof UniversalTable.ChildRelationship) {
        UniversalTable.ChildRelationship rel = (UniversalTable.ChildRelationship) field;

        RelationType relType = createFields(rel.getChildTable().getFields().getFields());
        fields.add(new Field(
            field.getName(),
            new SchemaElementDescription(""),
            null,
            List.of(new FieldType(
                field.getName(),
                relType,
                rel.getMultiplicity() == Multiplicity.MANY ? 1 : 0, //todo Array
                List.of()// : List.of(NotNull.INSTANCE)
            ))
        ));
      } else {
        throw new RuntimeException();
      }
    }
    return new RelationType<>(fields);
  }

  private Type toType(Column type) {
    switch (type.getType().getSqlTypeName()) {
      case BOOLEAN:
        return new BooleanType();
      case TINYINT:
      case SMALLINT:
      case BIGINT:
      case INTEGER:
      case DATE:
      case TIMESTAMP:
        return new IntegerType();
      case CHAR:
      case VARCHAR:
        if (type.getType().getPrecision() == 32) {
          return new UuidType();
        }
        return new StringType();
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
        return new FloatType();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return new DateTimeType();
      case TIME:
      case BINARY:
      case VARBINARY:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY:
      case NULL:
      case SYMBOL:
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }

  private TableSource productTableSource() {
    RelDataTypeFactory typeFactory = TypeFactory.getTypeFactory();

    Function<Boolean, UniversalTable> createTable = (source) -> {
      ImportFactory importFactory =
          new UniversalTable.ImportFactory(typeFactory, true, source);

      UniversalTable table = importFactory.createTable(Name.system("product"),
          NamePath.of("product"),
          source);

      table.addColumn(Name.system("productid"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
              false));
      table.addColumn(Name.system("name"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR),
              false));
      table.addColumn(Name.system("description"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR),
              false));
      table.addColumn(Name.system("category"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR),
              false));
      return table;
    };

    TableConfig tableConfig = new TableConfig(ExternalDataType.source,
        CanonicalizerConfiguration.system,
        "UTF-8", new CSVFormat.Configuration(",", "", new String[]{"productid", "name", "description", "category"}),
        "product", "product", "fixed",
        new InMemConnector("in-mem-product", getProductData()));

    return tableConfig.initializeSource(ErrorCollector.root(),
        NamePath.of("ecommerce-data"),
        createFlexibleSchema(createTable.apply(false)));
  }

  public static TimeAnnotatedRecord<String>[] getOrderData() {
    return new TimeAnnotatedRecord[]{
        new TimeAnnotatedRecord<String>(
            "{\"id\":10007543,\"customerid\":1000101,\"time\":\"2022-05-19T01:29:39.553244Z\",\"entries\":[{\"productid\":7235,\"quantity\":1,\"unit_price\":17.35,\"discount\":0},{\"productid\":8757,\"quantity\":2,\"unit_price\":57.5,\"discount\":11.5}]}"),
        new TimeAnnotatedRecord<String>(
            "{\"id\":10008434,\"customerid\":1000107,\"time\":\"2022-05-19T01:45:39.553244Z\",\"entries\":[{\"productid\":3571,\"quantity\":1,\"unit_price\":41.95,\"discount\":0}]}"),
        new TimeAnnotatedRecord<String>(
            "{\"id\":10008231,\"customerid\":1000121,\"time\":\"2022-05-19T02:16:39.553244Z\",\"entries\":[{\"productid\":7552,\"quantity\":3,\"unit_price\":25.5,\"discount\":15},{\"productid\":3225,\"quantity\":1,\"unit_price\":105,\"discount\":0}]}"),
        new TimeAnnotatedRecord<String>(
            "{\"id\":10007140,\"customerid\":1000107,\"time\":\"2022-05-19T02:28:39.553244Z\",\"entries\":[{\"productid\":1332,\"quantity\":8,\"unit_price\":8.49},{\"productid\":3571,\"quantity\":1,\"unit_price\":41.95,\"discount\":5}]}")
    };
  }

  public static TimeAnnotatedRecord<String>[] getProductData() {
    return new TimeAnnotatedRecord[]{
//        new TimeAnnotatedRecord<String>(
//            "productid, name, description, category\n"),
        new TimeAnnotatedRecord<String>(
            "3571, Poptech Blow 500, High powered blowdryer for any hair, Personal Care\n"),
        new TimeAnnotatedRecord<String>(
            "7552, Simer Garden Hose 50ft, Long garden hose that rolls up, House & Garden\n"),
        new TimeAnnotatedRecord<String>(
            "8757, Original German Nutcracker 3ft, Hand-made nutcracker is the perfect Christmas decoration, Decoration\n"),
        new TimeAnnotatedRecord<String>(
            "7235, Aachen Snow Globe, Picturesque city in a beautiful snow globe, Decoration\n"),
        new TimeAnnotatedRecord<String>(
            "1332, Heavy Duty Butt Wipes, Wipes for hardiest of messes, Personal Care\n"),
        new TimeAnnotatedRecord<String>(
            "3225, 4ft Garden Gnome, A real-sized garden gnome adds character to your outdoor space, House & Garden\n")
    };
  }

  public static TimeAnnotatedRecord<String>[] getCustomerData() {
    return new TimeAnnotatedRecord[]{
        new TimeAnnotatedRecord<String>(
            "customerid, email, name, lastUpdated\n"),
        new TimeAnnotatedRecord<String>(
            "1000101, john.mekker@gmail.com, John Mekker, 1645396849\n"),
        new TimeAnnotatedRecord<String>(
            "1000107, emily.ludies@hotmail.com, Emily F. Ludies, 1650493189\n"),
        new TimeAnnotatedRecord<String>(
            "1000121, lalelu@hottunes.org, Michelle Dagnes, 1650493449\n"),
        new TimeAnnotatedRecord<String>("1000131, hotbear753@yahoo.com, Mark Spielman, 1650494449")
    };
  }

  //todo: Hacky way to get different in-mem sources to load
  @AutoService(SourceFactory.class)
  public static class InMemProduct extends InMemSourceFactory {

    public InMemProduct() {
      super("in-mem-product");
    }
  }

  @AutoService(SourceFactory.class)
  public static class InMemOrders extends InMemSourceFactory {

    public InMemOrders() {
      super("in-mem-orders");
    }
  }

  @AutoService(SourceFactory.class)
  public static class InMemCustomer extends InMemSourceFactory {

    public InMemCustomer() {
      super("in-mem-customer");
    }
  }

  private TableSource customerTableSource() {
    RelDataTypeFactory typeFactory = TypeFactory.getTypeFactory();

    Function<Boolean, UniversalTable> createTable = (source) -> {
      ImportFactory importFactory =
          new UniversalTable.ImportFactory(typeFactory, true, source);

      UniversalTable table = importFactory.createTable(Name.system("customer"),
          NamePath.of("customer"),
          source);

      table.addColumn(Name.system("customerid"), typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.BIGINT), false));
      table.addColumn(Name.system("email"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR),
              false));
      table.addColumn(Name.system("name"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR),
              false));
      table.addColumn(Name.system("lastUpdated"),
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
              false));
      return table;
    };

    TableConfig tableConfig = new TableConfig(ExternalDataType.source,
        CanonicalizerConfiguration.system,
        "UTF-8", new CSVFormat.Configuration(",", "", new String[]{"customerid", "email", "name", "lastUpdated"}),
        "customer", "customer", "fixed",
        new InMemConnector("in-mem-customer", getCustomerData()));

    return tableConfig.initializeSource(ErrorCollector.root(),
        NamePath.of("ecommerce-data"),
        createFlexibleSchema(createTable.apply(false)));
  }

  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    return Optional.ofNullable(tables.get(name));
  }

  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return new ArrayList<>(tables.values());
  }
}
