package com.datasqrl.plan.local.analyze;

import com.datasqrl.config.SourceFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.InMemSourceFactory;
import com.datasqrl.io.formats.CSVFormat;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.impl.CanonicalizerConfiguration;
import com.datasqrl.io.impl.inmem.InMemConnector;
import com.datasqrl.io.tables.TableConfig;
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
import com.datasqrl.schema.UniversalTable.ImportFactory;
import com.google.auto.service.AutoService;
import java.lang.reflect.ParameterizedType;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

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
          LocalDateTime.parse("2022-05-19T01:29:39.553244Z", DateTimeFormatter.ISO_DATE_TIME),
          Arrays.asList(new Entries(7235, 1, 17.35, 0.0), new Entries(8757, 2, 57.5, 11.5))),
      new Orders(10008434, 1000107,
          LocalDateTime.parse("2022-05-19T01:45:39.553244Z", DateTimeFormatter.ISO_DATE_TIME),
          Arrays.asList(new Entries(3571, 1, 41.95, 0.0))),
      new Orders(10008231, 1000121,
          LocalDateTime.parse("2022-05-19T02:16:39.553244Z", DateTimeFormatter.ISO_DATE_TIME),
          Arrays.asList(new Entries(7552, 3, 25.5, 15.0), new Entries(3225, 1, 105.0, 0.0))),
      new Orders(10007140, 1000107,
          LocalDateTime.parse("2022-05-19T02:28:39.553244Z", DateTimeFormatter.ISO_DATE_TIME),
          Arrays.asList(new Entries(1332, 8, 8.49, 0.0), new Entries(3571, 1, 41.95, 5.0))));

  static List<Customer> customers = List.of(
      new Customer(1000101, "john.mekker@gmail.com", "John Mekker", 1645396849),
      new Customer(1000107, "emily.ludies@hotmail.com", "Emily F. Ludies", 1650493189),
      new Customer(1000121, "lalelu@hottunes.org", "Michelle Dagnes", 1650493449),
      new Customer(1000131, "hotbear753@yahoo.com", "Mark Spielman", 1650494449));

  static NamespaceObject customerNs = new TableSourceNamespaceObject(
      ReflectionToTableSource.createTableSource(Customer.class, customers));
  static NamespaceObject ordersNs = new TableSourceNamespaceObject(
      ReflectionToTableSource.createTableSource(Orders.class, orders));
  static NamespaceObject productNs = new TableSourceNamespaceObject(
      ReflectionToTableSource.createTableSource(Product.class, products));

  private final Map<Name, NamespaceObject> tables;

  public RetailSqrlModule() {
    this.tables = Stream.of(customerNs, ordersNs, productNs)
        .collect(Collectors.toMap(e -> e.getName(), e -> e));
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
    LocalDateTime time;
    List<Entries> entries;
  }

  @Value
  public static class Entries {

    long productid;
    int quantity;
    double unit_price;
    double discount;
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
}
