package com.datasqrl.data;/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
import java.io.*;
import java.util.regex.*;
import java.util.*;
import java.util.stream.*;
import java.time.Instant;

/**
 * Utility class used to generate the nut products from the USDA listing of nuts (in the file
 * nuts_usda.json in the data directory) as well as a random list of orders.
 * <p>
 * In the sibling data directory, run the following command: java ../util/GenerateNutData.java
 * ../nutshop-data/nuts_usda.json [num_customers] [num_days] [orders_per_day]
 * <p>
 * Note, that this generator is randomized and will produce a different listing of products with
 * each run. It will contain all of the same nuts but with different sizes.
 */
public class GenerateSeedData {

  static final Pattern[] patterns = new Pattern[]{
      Pattern.compile("\"description\":\"([^\"]*)\""),
      Pattern.compile("\"fdcId\":([\\d]*)")
  };

  static final String csv_delimiter = "; ";

  static final Random rand = new Random();

  static final String[] product_header = {"id", "name", "sizing", "weight_in_gram", "type",
      "category", "usda_id", "updated"};

  static final Size defaultSize = new Size("1 lbs", 454, 1.0);
  static final Size[] other_sizes = new Size[]{
      new Size("2 lbs", 907, 1.7),
      new Size("5 lbs", 2268, 3.8),
      new Size("10 lbs", 4536, 7.4),
      new Size("bulk", 25000, 31.2)
  };

  static final String productsFilename = "products.csv";
  static final String ordersFilenamePrefix = "orders_part";
  static final String ordersFilenameSuffix = ".json";

  static final long orderNoStart = 150000000l;
  static final int ordersPerFile = 1000000;
  static final double ordersStdDevPercentage = 0.4;
  static final double numEntriesStdDev = 0.8;
  static final double quantityStdDev = 1;
  static final double discountLikelihood = 0.4;
  static final double maxDiscountPercentage = 0.5;
  static final double minDiscountPercentage = 0.05;
  static final boolean useEpochForOrders = false;


  public static int sampleCustomerId(int numCustomers) {
    return rand.nextInt(numCustomers) + 1;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      throw new IllegalArgumentException(
          "Expected arguments: [Nut Filename] [Num Customers] [Num Days] [Orders Per Day]");
    }
    final String nutFile = args[0];
    final int numCustomers = Integer.parseInt(args[1]);
    final long numDays = Integer.parseInt(args[2]);
    final int ordersPerDay = Integer.parseInt(args[3]);

    if (numDays < 1 || numCustomers < 1 || ordersPerDay < 1) {
      throw new IllegalArgumentException("Numbers must be positive integers");
    }

    final long timeNow = System.currentTimeMillis();
    final int millisPerDay = (24 * 3600 * 1000);
    final long productUpdateTime = timeNow - (numDays + 1) * millisPerDay;
    System.out.println(String.format("Before time %s as %s", productUpdateTime, Instant.ofEpochMilli(productUpdateTime)));

    //First, generate products from list of USDA nuts
    List<Product> products = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(nutFile))) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        boolean match = true;
        String[] matches = new String[patterns.length];
        for (int i = 0; i < patterns.length; i++) {
          Pattern p = patterns[i];
          Matcher m = p.matcher(line);
          if (m.find()) {
            matches[i] = m.group(1);
          } else {
            match = false;
          }
        }
        if (match) {
          String[] record = new String[product_header.length];
          String[] desc_comps = matches[0].split(", ");
          String name = String.join(", ", Arrays.copyOfRange(desc_comps, 1, desc_comps.length));
          double price = rand.nextDouble() * 8.0 + 5.0;
          Product product = new Product(name, desc_comps[0], desc_comps[1], price,
              defaultSize, matches[1], Instant.ofEpochMilli(productUpdateTime));
          products.add(product);

          int variants = 0;
          if (rand.nextDouble() > 0.5) {
            variants = rand.nextInt(other_sizes.length);
          }
          for (int i = 0; i < variants; i++) {
            products.add(product.changeSize(other_sizes[i]));
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    writeToFile(products, productsFilename, String.join(csv_delimiter, product_header), null);

    //Second, generate orders
    int daysPerFile = Math.max(1, ordersPerFile / ordersPerDay);
    int numFiles = 0;
    List<Order> orders = new ArrayList<>();
    final double stdDevMulti = ordersPerDay * ordersStdDevPercentage;
    for (int days = 0; days < numDays; days++) {
      if ((days + 1) % daysPerFile == 0) {
        writeOrdersToFile(orders, ++numFiles);
        orders.clear();
      }
      long baseTime = timeNow - (numDays - days) * millisPerDay;
//      System.out.println(String.format("Base time %s as %s", baseTime, Instant.ofEpochMilli(baseTime)));
      int numOrders = ordersPerDay + (int) Math.round(rand.nextGaussian() * stdDevMulti);

      for (int i = 0; i < numOrders; i++) {
        int numEntries = Math.min(
            Math.round(1 + (int) Math.abs(rand.nextGaussian() * numEntriesStdDev)),
            products.size() / 8);
        Collection<Product> selectProducts = sampleWithoutReplacement(products, numEntries);
        List<Order.Entry> entries = new ArrayList<>();
        for (Product p : selectProducts) {
          int quantity = Math.round(1 + (int) Math.abs(rand.nextGaussian() * quantityStdDev));
          double unitPrice = p.price;
          double total = quantity * unitPrice;
          double discount = 0.0;
          if (rand.nextDouble() < discountLikelihood) {
            double percentage = minDiscountPercentage + rand.nextDouble() * (maxDiscountPercentage
                - minDiscountPercentage);
            discount = percentage * total;
          }
          entries.add(new Order.Entry(p.id, quantity, unitPrice, discount));
        }
        long epochMillis = baseTime + rand.nextInt(millisPerDay);
        Comparable timestamp = Long.valueOf(epochMillis);
        if (!useEpochForOrders) {
          timestamp = Instant.ofEpochMilli(epochMillis);
        }
        orders.add(new Order(sampleCustomerId(numCustomers), timestamp, entries));
      }

    }
    if (!orders.isEmpty()) {
      writeOrdersToFile(orders, ++numFiles);
    }
  }

  public static <T> Collection<T> sampleWithoutReplacement(List<T> sample, int no) {
    assert no > 0 && sample.size() >= no;
    List<T> result = new ArrayList<T>();
    for (int i = 0; i < no; i++) {
      T s = drawRandom(sample);
      while (result.contains(s)) {
        s = drawRandom(sample);
      }
      result.add(s);
    }
    return result;
  }

  public static <T> T drawRandom(List<T> sample) {
    int offset = rand.nextInt(sample.size());
    return sample.get(offset);
  }

  public static void writeOrdersToFile(List<Order> orders, int fileNo) {
    String ordersFilename = ordersFilenamePrefix + fileNo + ordersFilenameSuffix;
    orders.sort((Order o1, Order o2) -> o1.timestamp.compareTo(o2.timestamp));
//        writeToFile(orders,ordersFilename,"[","]");
    writeToFile(orders, ordersFilename, null, null);
  }

  public static void writeToFile(List<? extends Object> objs, String filename, String header,
      String footer) {
    System.out.printf("Writing %s elements to file %s\n", objs.size(), filename);
    try (PrintWriter out = new PrintWriter(new FileWriter(filename))) {
      if (header != null) {
        out.println(header);
      }
      for (Object o : objs) {
        out.println(o);
      }
      if (footer != null) {
        out.println(footer);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static class Size {

    public final String name;
    public final int grams;
    public final double price_multi;

    public Size(String name, int grams, double price_multi) {
      this.name = name;
      this.grams = grams;
      this.price_multi = price_multi;
    }

  }

  public static class Order {

    private static long counter = orderNoStart;

    public final long id;
    public final int customerid;
    public final Comparable timestamp;
    public final List<Entry> entries;

    public Order(int customerid, Comparable timestamp, List<Entry> entries) {
      this.id = ++counter;
      this.customerid = customerid;
      this.timestamp = timestamp;
      this.entries = entries;
    }

    public String toString() {
      String timestampPattern = (timestamp instanceof Number)?"%d":"\"%s\"";
      return String.format("{\"id\": %d, \"customerid\": %d, \"time\": "+timestampPattern+", \"items\": [%s] }", id,
          customerid, timestamp,
          String.join(",", entries.stream().map(e -> e.toString()).collect(Collectors.toList())));
    }

    public static class Entry {

      public final int productid;
      public final int quantity;
      public final double unitPrice;
      public final double discount;

      public Entry(int productid, int quantity, double unitPrice, double discount) {
        this.productid = productid;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.discount = discount;
      }

      public String toString() {
        String result = String.format("{\"productid\": %d,\"quantity\": %d,\"unit_price\": %5.2f ",
            productid, quantity, unitPrice);
        if (discount > 0) {
          result += String.format(",\"discount\": %5.2f ", discount);
        }
        result += "}";
        return result;
      }

    }

  }

  public static class Product {

    private static int counter = 0;

    public final int id;
    public final String name;
    public final String type;
    public final String category;
    public final double price;
    public final Size size;
    public final String usdaId;
    public final Instant updated;

    public Product(String name, String type, String category, double price, Size size,
        String usdaId, Instant updated) {
      this.id = ++counter;
      this.name = name;
      this.type = type;
      this.category = category;
      this.price = price;
      this.size = size;
      this.usdaId = usdaId;
      this.updated = updated;
    }

    public Product changeSize(Size size) {
      return new Product(this.name, this.type, this.category, this.price * size.price_multi,
          size, this.usdaId, this.updated);
    }

    public String toString() {
      return String.join(csv_delimiter,
          new String[]{String.valueOf(id), name, size.name, String.valueOf(size.grams),
              type, category, usdaId, String.valueOf(updated)});
    }

    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      if (!super.equals(object)) {
        return false;
      }
      Product product = (Product) object;
      return id == product.id;
    }

    public int hashCode() {
      return Objects.hash(super.hashCode(), id);
    }
  }

}