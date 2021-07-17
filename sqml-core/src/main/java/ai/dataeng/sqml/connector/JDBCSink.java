package ai.dataeng.sqml.connector;

import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.Session;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.Connection;
import java.sql.DriverManager;

public class JDBCSink extends Sink {

  private final String driver;
  private final String url;

  @JsonCreator
  public JDBCSink(
      @JsonProperty("driver") String driver,
      @JsonProperty("url") String url) {
    this.driver = requireNonNull(driver, "driver is null");
    this.url = requireNonNull(url, "url is null");
  }

  public String getDriver() {
    return driver;
  }

  public String getUrl() {
    return url;
  }


  public Session createSession() {
    return new Session(connect());
  }

  public Connection connect() {
    Connection c;
    try {
      Class.forName(getDriver());
      c = DriverManager
          .getConnection(getUrl());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("Opened database successfully");
    return c;
  }
}
