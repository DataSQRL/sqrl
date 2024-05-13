//package com.datasqrl.plan.local.analyze;
//
//import com.datasqrl.calcite.type.NamedRelDataType;
//import com.datasqrl.canonicalizer.Name;
//import com.datasqrl.canonicalizer.NamePath;
//import com.datasqrl.config.SourceFactory;
//import com.datasqrl.io.DataSystemConnectorFactory;
//import com.datasqrl.io.InMemSourceFactory;
//import com.datasqrl.discovery.system.FileTableConfigFactory;
//import com.datasqrl.io.mem.MemoryConnectorFactory;
//import com.datasqrl.io.tables.TableSource;
//import com.datasqrl.loaders.TableSourceNamespaceObject;
//import com.datasqrl.module.NamespaceObject;
//import com.datasqrl.module.SqrlModule;
//import com.datasqrl.plan.table.CalciteTableFactory;
//import com.datasqrl.schema.converters.RelDataTypeToFlexibleSchema;
//import com.google.auto.service.AutoService;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.time.ZonedDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//import lombok.Value;
//import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
//
//public class FuzzingRetailSqrlModule implements SqrlModule {
//  static List<Product> products = List.of(
//      new Product(3571, "Poptech Blow 500", "High powered blowdryer for any hair", "Personal Care"),
//      new Product(7552, "Simer Garden Hose 50ft", "Long garden hose that rolls up", "House & Garden"),
//      new Product(8757, "Original German Nutcracker 3ft", "Hand-made nutcracker is the perfect Christmas decoration", "Decoration"),
//      new Product(7235, "Aachen Snow Globe", "Picturesque city in a beautiful snow globe", "Decoration"),
//      new Product(1332, "Heavy Duty Butt Wipes", "Wipes for hardiest of messes", "Personal Care"),
//      new Product(3225, "4ft Garden Gnome", "A real-sized garden gnome adds character to your outdoor space", "House & Garden"),
//      new Product(1, "Product with 0 price", "Test product", "Test Category"),
//      new Product(2, "Product with negative price", "Test product", "Test Category"),
//      new Product(3, "Product with very high price", "Test product", "Test Category"),
//      new Product(4, "Product with very high discount", "Test product", "Test Category"),
//      new Product(5, "Product with negative discount", "Test product", "Test Category"),
//      new Product(6, "Product with large quantity", "Test product", "Test Category")
//  );
//
//  static List<Orders> orders = new ArrayList<>(List.of(
//      createOrder("simple-two-item-order", 1000101, "2023-05-19T01:29:39.553244Z", List.of(createEntry(7235, 1, 17.35, 0.0), createEntry(8757, 2, 57.5, 11.5))),
//      createOrder("simple-single-item-order", 1000107, "2023-05-19T01:45:39.553244Z", List.of(createEntry(3571, 1, 41.95, 0.0))),
//      createOrder("precision-loss-trailing-decimal-order", 1000121, "2023-05-19T02:16:39.553244Z", List.of(createEntry(7552, 3, 25.5, 15.0), createEntry(3225, 1, 3.33333333333333333333, 0.0))),
//      createOrder("precision-loss-leading-decimal-order", 1000121, "2023-05-19T02:16:39.553244Z", List.of(createEntry(7552, 3, 25.5, 15.0), createEntry(3225, 1, 33333333333333333333.3, 0.0))),
//      createOrder("order-with-null-discount", 1000107, "2023-05-19T02:28:39.553244Z", List.of(createEntry(1332, 8, 8.49, null), createEntry(3571, 1, 41.95, 5.0))),
//      createOrder("order-with-variety-of-values", 1000150, "2023-06-19T01:29:39.553244Z", List.of(createEntry(1, 10, 0.0, null), createEntry(2, 20, -100.0, 0.0), createEntry(3, 30, 1000000.0, 0.0), createEntry(4, 40, 500.0, 900.0), createEntry(5, 50, 100.0, -50.0), createEntry(6, 1000000, 1.0, 0.0))),
//      createOrder(/*duplicate*/"simple-two-item-order", 1000101, "2023-05-19T01:29:39.553244Z", List.of(createEntry(7235, 1, 17.35, 0.0), createEntry(8757, 2, 57.5, 11.5))),
//      createOrder(/*duplicate*/"simple-single-item-order", 1000107, "2023-05-19T01:45:39.553244Z", List.of(createEntry(3571, 1, 41.95, 0.0))),
//      createOrder("simple-two-item-order-same-customer", 1000121, "2023-05-19T02:16:39.553244Z", List.of(createEntry(7552, 3, 25.5, 15.0), createEntry(3225, 1, 105.0, 0.0))),
//      createOrder("order-with-multiple-entries", 1000151, "2023-06-19T02:29:39.553244Z", IntStream.rangeClosed(1, 50).mapToObj(i -> createEntry(i, 1, 10.0, 0.0)).collect(Collectors.toList())),
//      createOrder("order-with-future-date", 1000153, "2025-06-19T01:29:39.553244Z", List.of(createEntry(1, 10, 0.0, null), createEntry(2, 2, -100.0, 0.0), createEntry(3, 30, 2.0, 0.0))),
//      createOrder("order-with-past-date", 1000154, "2021-06-19T01:29:39.553244Z", List.of(createEntry(1, 10, 0.0, null), createEntry(2, 20, -100.0, 0.0), createEntry(3, 30, 2.0, 0.0))),
//      createOrder("order-with-max-int-quantity", 1000107, "2023-05-19T02:28:39.553244Z", List.of(createEntry(1332, 8, 8.49, null), createEntry(3571, Integer.MAX_VALUE, 41.95, 5.0))),
//      createOrder("order-with-decimal", 1000150, "2023-06-19T01:29:39.553244Z", List.of(createEntry(1, 10, 0.0, null), createEntry(2, 20, 1000000000000.123456789d, 0.0), createEntry(3, 30, -1000000000000.123456789d, 0.0), createEntry(4, 40, 500.0, 900.0), createEntry(5, 50, 100.0, -50.0), createEntry(6, 1000000, 1.0, 0.0)))
//  ));
//
//  public static Orders createOrder(String orderId, long customerId, String zonedDateTime, List<Entries> entries) {
//    return new Orders(orderId, customerId,
//        ZonedDateTime.of(LocalDateTime.parse(zonedDateTime, DateTimeFormatter.ISO_DATE_TIME), ZoneId.of("UTC")),
//        entries);
//  }
//
//  public static Entries createEntry(long productId, int quantity, double unitPrice, Double discount) {
//    return new Entries(productId, quantity, unitPrice, Optional.ofNullable(discount));
//  }
//  static List<Customer> customers = List.of(
//      new Customer(1000101, "john.mekker@gmail.com", "John Mekker", 1645396849),
//      new Customer(1000107, "emily.ludies@hotmail.com", "Emily F. Ludies", 1650493189),
//      new Customer(1000121, "lalelu@hottunes.org", "Michelle Dagnes", 1650493449),
//      new Customer(1000131, "hotbear753@yahoo.com", "Mark Spielman", 1650494449),
//      new Customer(1000150, "test.user@gmail.com", "Test User", 1650800000),
//      new Customer(1000151, "user.with.many.orders@gmail.com", "Many Orders User", 1650900000),
//      new Customer(1000152, "customer.without.orders@gmail.com", "Without Orders User", 1650990000),
//      new Customer(1000153, "customer.with.future.orders@gmail.com", "Future Orders User", 1651000000),
//      new Customer(1000154, "customer.with.past.orders@gmail.com", "Past Orders User", 1651100000)
//  );
//
//  private static final Map<Class, List<?>> dataByClass = ImmutableMap.of(Customer.class, customers,
//          Orders.class, orders,
//          Product.class, products);
//
//  public static final String DATASET_NAME = "retail-large";
//  private static final Map<Name, NamespaceObject> tables = new HashMap<>();
//  private static final Map<String, List<?>> tableData = new HashMap<>();
//
//  public void init(CalciteTableFactory tableFactory) {
//    dataByClass.forEach((clazz, data) -> {
//      NamespaceObject obj = new TableSourceNamespaceObject(
//          createTableSource(clazz, DATASET_NAME), tableFactory, moduleLoader);
//      tables.put(obj.getName(), obj);
//      tableData.put(obj.getName().getCanonical(), data);
//    });
//  }
//
//  public static TableSource createTableSource(Class clazz, String datasetName) {
//    String name = clazz.getSimpleName().toLowerCase();
//    return new FileTableConfigFactory().forMock(name).build()
//        .initializeSource(NamePath.of("ecommerce-data-large"),
//            RelDataTypeToFlexibleSchema.createFlexibleSchema(new NamedRelDataType(Name.system(clazz.getSimpleName()),
//                ReflectionToRelDataType.reflectionToRelType(clazz))));
//  }
//
//  @Override
//  public Optional<NamespaceObject> getNamespaceObject(Name name) {
//    return Optional.ofNullable(tables.get(name));
//  }
//
//  @Override
//  public List<NamespaceObject> getNamespaceObjects() {
//    return new ArrayList<>(tables.values());
//  }
//
//  @Value
//  public static class Orders {
//
//    String id;
//    long customerid;
//    ZonedDateTime time;
//    List<Entries> entries;
//  }
//
//  @Value
//  public static class Entries {
//
//    long productid;
//    int quantity;
//    double unit_price;
//    Optional<Double> discount;
//  }
//
//  // Create the class instances for each of the 4 records
//  @Value
//  public static class Customer {
//
//    private int customerid;
//    private String email;
//    private String name;
//    private long lastUpdated;
//  }
//
//
//  @Value
//  public static class Product {
//
//    int productid;
//    String name;
//    String description;
//    String category;
//  }
//
//
//  //todo: Hacky way to get different in-mem sources to load
//  @AutoService(SourceFactory.class)
//  public static class InMemCustomer extends InMemSourceFactory {
//
//    public InMemCustomer() {
//      super(DATASET_NAME, tableData);
//    }
//  }
//
//  @AutoService(DataSystemConnectorFactory.class)
//  public static class InMemConnector extends MemoryConnectorFactory {
//
//    public InMemConnector() {
//      super(DATASET_NAME);
//    }
//  }
//}
