package com.datasqrl.data;

import com.datasqrl.cmd.AbstractGenerateCommand;
import com.datasqrl.util.Configuration;
import com.datasqrl.util.RandomSampler;
import com.datasqrl.util.SerializerUtil;
import com.datasqrl.util.WriterUtil;
import com.github.javafaker.CreditCardType;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Value;
import picocli.CommandLine;

@CommandLine.Command(name = "creditcard", description = "Generates Credit Card data")
public class GenerateCreditCards extends AbstractGenerateCommand {

  public static final String TRANSACTIONS_FILE = "transaction_part%04d.json";

  public static final String CARDASSIGNMENT_FILE = "assignment_part%04d.json";

  public static final String MERCHANT_FILE = "merchant.json";

  @Override
  public void run() {
    initialize();
    Config config = getConfiguration(new Config());

    long numDays = (long)Math.ceil(Math.max(1,root.getNumber()/
        (config.numCustomers*config.avgCardsPerCustomer*config.avgTransactionPerDay)));
    Instant startTime = getStartTime(numDays);
    Instant initialAssignment = startTime.minus(1, ChronoUnit.DAYS);


    Map<Merchant, MerchantCategory> merchants = new HashMap<>(config.numMerchants);
    IntStream.range(0, config.numMerchants).forEach(i -> {
      MerchantCategory category = MerchantCategory.get(sampler);
      Merchant merchant = new Merchant(i+1, faker.company().name(), category.name, initialAssignment.toString());
      merchants.put(merchant, category);
    });
    List<Merchant> merchantList = merchants.keySet().stream().collect(Collectors.toUnmodifiableList());

    List<Integer> customerIds = IntStream.range(1, config.numCustomers+1).boxed().collect(
        Collectors.toUnmodifiableList());

    List<CardAssignment> assignments = new ArrayList<>();
    Map<String, Integer> cardAssignment = new HashMap<>();
    for (int customerid : customerIds) {
      int numCards = (int)Math.max(1,Math.round(sampler.nextNormal(config.avgCardsPerCustomer, config.avgCardsPerCustomerDeviation)));
      for (int i = 0; i < numCards; i++) {
        String cardNo = generateNewCardNo(cardAssignment.keySet());
        cardAssignment.put(cardNo, customerid);
        assignments.add(new CardAssignment(customerid, cardNo, initialAssignment.toString()));
      }
    }

    WriterUtil.writeToFile(merchants.keySet().stream().collect(Collectors.toUnmodifiableList()), getOutputDir().resolve(MERCHANT_FILE), null, null);
    WriterUtil.writeToFile(assignments, getOutputDir().resolve(String.format(CARDASSIGNMENT_FILE,0)), null, null);

    long totalRecords = 0;
    Instant startOfDay = startTime;
    long transactionId = sampler.nextLong(100000,10000000L);
    for (int i = 0; i < numDays; i++) {
      //Transactions
      List<Transaction> transactions = new ArrayList<>();
      for (String cardNo : cardAssignment.keySet()) {
        int numTx = (int)Math.round(sampler.nextPositiveNormal(config.avgTransactionPerDay, config.avgTransactionPerDayDeviation));
        for (int t = 0; t < numTx; t++) {
          Instant timestamp = sampler.nextTimestamp(startOfDay, 1, ChronoUnit.DAYS);
          Merchant merchant = sampler.next(merchantList);
          MerchantCategory category = merchants.get(merchant);
          transactions.add(new Transaction(transactionId++, cardNo, timestamp.toString(), category.getAmount(sampler), merchant.merchantId));
        }
      }
      WriterUtil.writeToFileSorted(transactions, getOutputDir().resolve(String.format(TRANSACTIONS_FILE,i+1)),
          Comparator.comparing(Transaction::getTime),
          null, null);
      totalRecords += transactions.size();

      startOfDay = startOfDay.plus(1, ChronoUnit.DAYS); //next day
      //Card updates happen at the end of the day (i.e. issuance and removing)

      int numUpdates = (int)Math.round(sampler.nextPositiveNormal(
          config.avgCardUpdates, config.avgCardUpdatesDeviation));
      List<CardAssignment> updates = new ArrayList<>();
      for (int j = 0; j < numUpdates; j++) {
        if (sampler.flipCoin(config.cardUpdateIssuanceProbability)) {
          //Issue new card
          Integer customerid = sampler.next(customerIds);
          String cardNo = generateNewCardNo(cardAssignment.keySet());
          cardAssignment.put(cardNo, customerid);
          updates.add(new CardAssignment(customerid, cardNo, startOfDay.toString()));
        } else {
          //Remove card
          String cardNo = sampler.next(cardAssignment.keySet());
          cardAssignment.remove(cardNo);
        }
      }
      if (i+1<numDays) { //only write if there are more transactions to come
        WriterUtil.writeToFileSorted(updates,
            getOutputDir().resolve(String.format(CARDASSIGNMENT_FILE, i + 1)),
            Comparator.comparing(CardAssignment::getTimestamp),
            null, null);
      }

    }
  }

  private static final List<CreditCardType> cardTypes = List.of(CreditCardType.MASTERCARD,
      CreditCardType.VISA, CreditCardType.AMERICAN_EXPRESS, CreditCardType.DISCOVER);

  public String generateNewCardNo(Set<String> existingCards) {
    String cardno;
    do {
      cardno = faker.finance().creditCard(sampler.next(cardTypes));
      cardno = cardno.replace("-","");
    } while (existingCards.contains(cardno));
    return cardno;
  }

  @Value
  public static class Transaction {

    long transactionId;
    String cardNo;
    String time;
    double amount;
    int merchantId;

    @Override
    public String toString() {
      return SerializerUtil.toJson(this);
    }

  }


  @Value
  public static class CardAssignment {

    int customerId;
    String cardNo;
    String timestamp;

    @Override
    public String toString() {
      return SerializerUtil.toJson(this);
    }

  }

  @Value
  public static class Merchant {

    int merchantId;
    String name;
    String category;
    String updatedTime;

    @Override
    public String toString() {
      return SerializerUtil.toJson(this);
    }

  }

  public static class Config implements Configuration {

    public int numCustomers = 10;

    public int numMerchants = 100;

    public double avgCardsPerCustomer = 1.2;

    public double avgCardsPerCustomerDeviation = 1;

    public int avgCardUpdates = 3;

    public int avgCardUpdatesDeviation = 1;

    public double cardUpdateIssuanceProbability = 0.8;

    public double avgTransactionPerDay = 10;

    public double avgTransactionPerDayDeviation = 10;



    @Override
    public void scale(long scaleFactor, long number) {
      numCustomers = (int)Math.min(10000000,Math.max(20, number/(86400*60)));
    }
  }

  public enum MerchantCategory {
    Rent("Rent & Mortgage", 300, 10000),
    Utilities("Utilities", 30, 600),
    Groceries("Groceries", 7, 800),
    Restaurants("Restaurants & Dining", 15, 400),
    Health("Health Insurance", 250, 1500),
    CarInsurance("Car Insurance", 20, 800),
    HomeInsurance("Home Insurance", 80, 500),
    LifeInsurance("Life Insurance", 15, 200),
    CarPayments("Car Payments", 150, 1200),
    Fuel("Fuel", 25, 120),
    PublicTransit("Public Transportation", 2, 30),
    Home("Home Maintenance & Repair", 200, 12000),
    CarMaintenance("Car Maintenance & Repair", 100, 5000),
    Wellness("Health & Wellness", 30, 200),
    Personal("Personal Care", 4, 50),
    Clothing("Clothing & Apparel", 20, 500),
    Entertainment("Entertainment", 5, 50),
    Hobbies("Hobbies", 2, 500),
    Education("Education", 200, 4000),
    Childcare("Childcare", 500, 4000),
    Pet("Pet Care", 10, 200),
    Travel("Travel & Vacations", 400, 10000),
    Gifts("Gifts & Donations", 10, 10000),
    Digital("Digital Services", 2, 200),
    Cell("Cell Phone Bill", 15, 250),
    Internet("Internet Bill", 20, 200),
    HomeDecor("Home Furnishing & Decor", 15, 1800),
    Miscellaneous("Miscellaneous", 1, 2000);

    public final String name;
    public final double min;
    public final double max;

    MerchantCategory(String name, double min, double max) {
      this.name = name;
      this.min = min;
      this.max = max;
    }

    public double getAmount(RandomSampler sampler) {
      return Math.round(sampler.nextDouble(min,max)*100.00)/100.0;
    }

    public static MerchantCategory get(RandomSampler sampler) {
      return values()[sampler.nextInt(0, values().length)];
    }


  }


}
