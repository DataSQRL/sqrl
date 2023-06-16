package com.datasqrl.data;

import com.datasqrl.cmd.AbstractGenerateCommand;
import com.datasqrl.data.GenerateSensors.MachineGroup;
import com.datasqrl.util.Configuration;
import com.datasqrl.util.Money;
import com.datasqrl.util.RandomSampler;
import com.datasqrl.util.SerializerUtil;
import com.datasqrl.util.WriterUtil;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import com.github.javafaker.Business;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import lombok.Value;
import picocli.CommandLine;

@CommandLine.Command(name = "loan", description = "Generates financial loan data")
public class GenerateLoans extends AbstractGenerateCommand {

  public static final String LOAN_TYPE_FILE = "loan_types.json";

  public static final String APPLICATIONS_FILE = "applications.json";

  public static final String APPLICATION_UPDATES_FILE = "application_updates.json";

  @Override
  public void run() {
    initialize();
    Config config = getConfiguration(new Config());
    long numLoans = root.getNumber();
    Instant startTime = getStartTime(config.numDaysPast);
    Instant endTime = Instant.now();
    List<LoanApplication> applications = new ArrayList<>();
    List<LoanApplicationUpdate> updates = new ArrayList<>();
    for (int i = 1; i < numLoans; i++) {
      LoanType loanType = loanTypes[sampler.nextInt(0, loanTypes.length)];
      int customerid = sampler.nextInt(0,config.numCustomers)+1;
      Instant appTime = sampler.nextTimestamp(startTime, endTime);
      LoanApplication app = new LoanApplication(i,customerid, loanType.id, loanType.generateAmount(sampler),
          loanType.generateDuration(sampler), appTime.toString(), appTime.toString());
      applications.add(app);
      int numUpdates = sampler.nextInt(1,config.review_steps.length);
      Instant nextTime = appTime;
      for (int j = 0; j < numUpdates; j++) {
        String step = config.review_steps[j];
        nextTime = sampler.nextTimestamp(nextTime, endTime);
        String message = faker.hobbit().quote();
        LoanApplicationUpdate update = new LoanApplicationUpdate(app.id, step, message, nextTime.toString());
        updates.add(update);
        if (sampler.flipCoin(config.updateProb)) {
          applications.add(app.with(loanType.generateAmount(sampler),nextTime));
        }
      }
    }
    WriterUtil.writeToFile(Arrays.asList(loanTypes), getOutputDir().resolve(LOAN_TYPE_FILE),
        null, null);
    WriterUtil.writeToFileSorted(applications, getOutputDir().resolve(APPLICATIONS_FILE),
        Comparator.comparing(LoanApplication::getUpdated_at),
        null, null);
    WriterUtil.writeToFileSorted(updates, getOutputDir().resolve(APPLICATION_UPDATES_FILE),
        Comparator.comparing(LoanApplicationUpdate::getTimestamp),
        null, null);
  }

  @Value
  public static class LoanType {

    int id;
    String name;
    String description;
    double interest_rate;
    double max_amount;
    double min_amount;
    int max_duration;
    int min_duration;
    String updated_at;

    public double generateAmount(RandomSampler sampler) {
      return Money.generate(min_amount, max_amount, sampler);
    }

    public int generateDuration(RandomSampler sampler) {
      return sampler.nextInt(min_duration,max_duration);
    }

    @Override
    public String toString() {
      return SerializerUtil.toJson(this);
    }

  }

  @Value
  public static class LoanApplication {

    int id;
    int customer_id;
    int loan_type_id;
    double amount;
    int duration;
    String application_date;
    String updated_at;

    public LoanApplication with(double amount, Instant updated_at) {
      return new LoanApplication(id, customer_id, loan_type_id, amount,
          duration, application_date, updated_at.toString());
    }

    @Override
    public String toString() {
      return SerializerUtil.toJson(this);
    }

  }

  @Value
  public static class LoanApplicationUpdate {

    int loan_application_id;
    String status;
    String message;
    String timestamp;

    @Override
    public String toString() {
      return SerializerUtil.toJson(this);
    }

  }

  public static String PAST_TIME = getStartTime(360).toString();

  public static LoanType[] loanTypes = new LoanType[]{
     new LoanType(1,"Personal Loan","Unsecured personal loans for various purposes",7.50,25000.00,1000.00,60,12, PAST_TIME),
      new LoanType( 2,"Home Loan","Loans for purchasing or refinancing a home",3.25,1000000.00,50000.00,360,60, PAST_TIME),
      new LoanType(3,"Auto Loan","Loans for purchasing new or used vehicles",4.00,75000.00,5000.00,72,24, PAST_TIME),
      new LoanType(4,"Student Loan","Loans for financing higher education",5.50,100000.00,2000.00,120,12, PAST_TIME),
      new LoanType(5,"Small Business Loan","Loans for starting or expanding a small business",6.75,500000.00,10000.00,120,24, PAST_TIME),
      new LoanType(6,"Debt Consolidation Loan","Loans for consolidating and refinancing existing debt",8.00,100000.00,5000.00,60,12, PAST_TIME),
      new LoanType(7,"Home Equity Loan","Loans based on the equity of a borrower's home",4.50,500000.00,10000.00,240,60, PAST_TIME),
      new LoanType(8,"Line of Credit","Revolving credit lines for personal or business use",6.00,100000.00,5000.00,60,12, PAST_TIME),
      new LoanType(9,"Construction Loan","Loans for financing home construction projects",5.25,750000.00,50000.00,24,6, PAST_TIME),
      new LoanType(10,"Medical Loan","Loans for financing medical expenses or procedures",7.75,50000.00,2000.00,60,12, PAST_TIME),
  };

  public static class Config implements Configuration {

    public int numCustomers = 10;

    public String[] review_steps = {"applied","reviewed","pre-approval","verification","check","underwriting","approval","processing","closing","funding","servicing"};

    public int numDaysPast = 30;

    public double updateProb = 0.05;

    @Override
    public void scale(long scaleFactor, long number) {
    }
  }

}
