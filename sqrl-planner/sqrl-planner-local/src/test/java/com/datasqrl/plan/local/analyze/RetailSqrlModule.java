package com.datasqrl.plan.local.analyze;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SourceFactory;
import com.datasqrl.io.DataSystemConnectorFactory;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.InMemSourceFactory;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.mem.MemoryConnectorFactory;
import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.schema.converters.UtbToFlexibleSchema;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.google.auto.service.AutoService;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import lombok.Value;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class RetailSqrlModule implements SqrlModule {

  static List<Product> products = List.of(
          new Product(3571, "Poptech Blow 500", "High powered blowdryer for any hair", "Personal Care"),
          new Product(7552, "Simer Garden Hose 50ft", "Long garden hose that rolls up",
                  "House & Garden"),
          new Product(8757, "Original German Nutcracker 3ft",
                  "Hand-made nutcracker is the perfect Christmas decoration", "Decoration"),
          new Product(7235, "Aachen Snow Globe", "Picturesque city in a beautiful snow globe",
                  "Decoration"),
          new Product(1332, "Heavy Duty Butt Wipes", "Wipes for hardiest of messes", "Personal Care"),
          new Product(3225, "4ft Garden Gnome",
                  "A real-sized garden gnome adds character to your outdoor space", "House & Garden"));
  static List<Orders> orders = List.of(
          new Orders(10007543, 1000101,
                  ZonedDateTime.of(LocalDateTime.parse("2023-05-19T01:29:39.553244Z", DateTimeFormatter.ISO_DATE_TIME), ZoneId.of("UTC")),
                  Arrays.asList(new Entries(7235, 1, 17.35, Optional.of(0.0)), new Entries(8757, 2, 57.5, Optional.of(11.5)))),
          new Orders(10008434, 1000107,
                  ZonedDateTime.of(LocalDateTime.parse("2023-05-19T01:45:39.553244Z", DateTimeFormatter.ISO_DATE_TIME), ZoneId.of("UTC")),
                  Arrays.asList(new Entries(3571, 1, 41.95, Optional.of(0.0)))),
          new Orders(10008231, 1000121,
                  ZonedDateTime.of(LocalDateTime.parse("2023-05-19T02:16:39.553244Z", DateTimeFormatter.ISO_DATE_TIME), ZoneId.of("UTC")),
                  Arrays.asList(new Entries(7552, 3, 25.5, Optional.of(15.0)), new Entries(3225, 1, 105.0, Optional.of(0.0)))),
          new Orders(10007140, 1000107,
                  ZonedDateTime.of(LocalDateTime.parse("2023-05-19T02:28:39.553244Z", DateTimeFormatter.ISO_DATE_TIME), ZoneId.of("UTC")),
                  Arrays.asList(new Entries(1332, 8, 8.49, Optional.empty()), new Entries(3571, 1, 41.95, Optional.of(5.0)))));

  static List<Customer> customers = List.of(
          new Customer(1000101, "john.mekker@gmail.com", "John Mekker", 1645396849),
          new Customer(1000107, "emily.ludies@hotmail.com", "Emily F. Ludies", 1650493189),
          new Customer(1000121, "lalelu@hottunes.org", "Michelle Dagnes", 1650493449),
          new Customer(1000131, "hotbear753@yahoo.com", "Mark Spielman", 1650494449));

  private static final Map<Class, List<?>> dataByClass = ImmutableMap.of(Customer.class, customers,
          Orders.class, orders,
          Product.class, products);

  public static final String DATASET_NAME = "retail";
  private static final Map<Name, NamespaceObject> tables = new HashMap<>();
  private static final Map<String, List<?>> tableData = new HashMap<>();

  public void init(CalciteTableFactory tableFactory) {
    dataByClass.forEach((clazz,data)-> {
      NamespaceObject obj = new TableSourceNamespaceObject(
              createTableSource(clazz, DATASET_NAME, "ecommerce-data"), tableFactory);
      tables.put(obj.getName(), obj);
      tableData.put(obj.getName().getCanonical(), data);
    });
  }



  public RetailSqrlModule() {

  }

  public static TableSource createTableSource(Class clazz, String datasetName, String moduleName) {
    String name = clazz.getSimpleName().toLowerCase();
    TableConfig.Builder builder = TableConfig.builder(name);
    builder.getFormatConfig().setProperty(FormatFactory.FORMAT_NAME_KEY, JsonLineFormat.NAME);
    builder.base(BaseTableConfig.builder().type(ExternalDataType.source.name()).identifier(name)
        .schema(FlexibleTableSchemaFactory.SCHEMA_TYPE).build());
    builder.getConnectorConfig().setProperty(DataSystemConnectorFactory.SYSTEM_NAME_KEY,"in-mem-" + datasetName);
    TableConfig tableConfig = builder.build();
    return tableConfig.initializeSource(NamePath.of(moduleName),
            UtbToFlexibleSchema.createFlexibleSchema(ReflectionToUt.reflection(clazz)));
  }

  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    return Optional.ofNullable(tables.get(name));
  }

  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return new ArrayList<>(tables.values());
  }

  @Value
  public static class Orders {

    long id;
    long customerid;
    ZonedDateTime time;
    List<Entries> entries;
  }

  @Value
  public static class Entries {

    long productid;
    int quantity;
    double unit_price;
    Optional<Double> discount;
  }

  // Create the class instances for each of the 4 records
  @Value
  public static class Customer {

    private int customerid;
    private String email;
    private String name;
    private long lastUpdated;
  }


  @Value
  public static class Product {

    int productid;
    String name;
    String description;
    String category;
  }


  //todo: Hacky way to get different in-mem sources to load
  @AutoService(SourceFactory.class)
  public static class InMemCustomer extends InMemSourceFactory {

    public InMemCustomer() {
      super(DATASET_NAME, tableData);
    }
  }

  @AutoService(DataSystemConnectorFactory.class)
  public static class InMemConnector extends MemoryConnectorFactory {

    public InMemConnector() {
      super(DATASET_NAME);
    }
  }
}
