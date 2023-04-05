package com.datasqrl.data;

import com.datasqrl.cmd.AbstractGenerateCommand;
import com.datasqrl.util.Configuration;
import com.datasqrl.util.StringTransformer;
import com.datasqrl.util.WriterUtil;
import com.github.javafaker.Book;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

@CommandLine.Command(name = "clickstream", description = "Generates clickstream data")
public class GenerateClickstream extends AbstractGenerateCommand {

  public static final String CONTENT_FILE = "content.csv";

  public static final String CLICK_FILE = "click_part%04d.csv";

  @Override
  public void run() {
    initialize();
    Config config = getConfiguration(new Config());

    long numDays = Math.max(1,root.getNumber()/config.avgClicksPerDay);
    Instant startTime = getStartTime(numDays);

    List<Content> contents = IntStream.range(0,config.numContent).mapToObj(i -> new Content(faker.book(), startTime))
        .collect(Collectors.toList());
    ListMultimap<Content,Content> transitionGraph = ArrayListMultimap.create();
    for (Content current : contents) {
      int numConnections = (int)Math.min(contents.size(),Math.max(1,Math.round(sampler.nextNormal(
          config.avgContentConnections,
          config.avgContentConnectionsDeviation))));
      transitionGraph.putAll(current, sampler.withoutReplacement(numConnections, contents));
    }

    WriterUtil.writeToFile(contents, getOutputDir().resolve(CONTENT_FILE), Content.header(), null);

    List<User> users = IntStream.range(0,config.numUsers).mapToObj(i -> new User(sampler.nextUUID()))
        .collect(Collectors.toList());

    long totalRecords = 0;
    Instant startOfDay = startTime;
    for (int i = 0; i < numDays; i++) {
      long records = Math.min(config.avgClicksPerDay, root.getNumber()-totalRecords);
      List<Click> clicks = new ArrayList<>((int)records);
      long sessions = Math.max(1,records/config.avgSessionClicks);
      for (int j = 0; j < sessions; j++) {
        User user = sampler.next(users);
        int sessionLength = (int)Math.round(sampler.nextPositiveNormal(config.avgSessionClicks,
            config.avgSessionClicksDeviation));
        Instant sessionTime = sampler.nextTimestamp(startOfDay,22,ChronoUnit.HOURS);
        Content content = sampler.next(contents);
        for (int k = 0; k < sessionLength; k++) {
          clicks.add(new Click(user, content, sessionTime));
          //walk to next content along graph
          content = sampler.next(transitionGraph.get(content));
          sessionTime = sampler.nextTimestamp(sessionTime, 2, ChronoUnit.MINUTES);
        }
      }
      WriterUtil.writeToFileSorted(clicks, getOutputDir().resolve(String.format(CLICK_FILE,i+1)),
          Comparator.comparing(Click::getTimestamp),
          Click.header(), null);
      startOfDay = startOfDay.plus(1, ChronoUnit.DAYS); //next day
      totalRecords+= clicks.size();
    }
  }

  public static class Content {

    String url;

    String category;

    String author;

    Instant updated;

    public Content(Book book, Instant updated) {
      this.url = StringTransformer.toURL(book.publisher(), book.title());
      this.category = book.genre();
      this.author = book.author();
      this.updated = updated;
    }

    @Override
    public String toString() {
      return StringUtils.join(new String[]{url, category, author, updated.toString()},", ");
    }

    public static String header() {
      return StringUtils.join(new String[]{"url", "category", "author", "updated"},", ");
    }

  }

  @Value
  public static class Click {

    String url;

    Instant timestamp;

    String userid;

    public Click(User user, Content content, Instant timestamp) {
      this.url = content.url;
      this.userid = user.id.toString();
      this.timestamp = timestamp;
    }

    @Override
    public String toString() {
      return StringUtils.join(new String[]{url, timestamp.toString(), userid},", ");
    }

    public static String header() {
      return StringUtils.join(new String[]{"url", "timestamp", "userid"},", ");
    }

  }

  @Value
  public static class User {

    UUID id;

  }

  public static class Config implements Configuration {

    public int numUsers = 30;

    public int avgSessionClicks = 5;

    public double avgSessionClicksDeviation = 1.0;

    public int numContent = 50;

    public int avgContentConnections = 7;

    public double avgContentConnectionsDeviation = 2.0;

    public int avgClicksPerDay = 100000;

    @Override
    public void scale(long scaleFactor, long number) {
      numUsers = numUsers * (int)Math.max(scaleFactor,1000000);
    }
  }


}
