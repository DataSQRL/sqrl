package ai.dataeng.sqml.connector;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HttpSource extends Source {
  private final String name;
  private final String source;

  @JsonCreator
  public HttpSource(
      @JsonProperty("name") String name,
      @JsonProperty("source") String source) {
    this.name = requireNonNull(name, "name is null");
    this.source = requireNonNull(source, "source is null");
  }

  public String getName() {
    return name;
  }

  public String getSource() {
    return source;
  }
}
