package ai.datasqrl.io.impl.kafka;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.formats.FileFormat;
import ai.datasqrl.io.formats.FormatConfiguration;
import ai.datasqrl.io.impl.file.FilePath;
import ai.datasqrl.io.sources.DataSourceConfiguration;
import ai.datasqrl.io.sources.DataSourceImplementation;
import ai.datasqrl.io.sources.SourceTableConfiguration;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.Admin;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class KafkaSourceImplementation implements DataSourceImplementation, Serializable {

  public static String[] TOPIC_SUFFIX = {".", "/", "_"};

  @NonNull @NotNull @NotEmpty
  List<String> servers;

  String topicPrefix;


  @Override
  public boolean initialize(ErrorCollector errors) {
    for (String server : servers) {
      if (Strings.isNullOrEmpty(server)) {
        errors.fatal("Invalid server configuration: %s", server);
      }
    }
    if (Strings.isNullOrEmpty(topicPrefix)) {
      topicPrefix = "";
    }

    //Check that we can connect to Kafka cluster
    try (Admin admin = Admin.create(getProperties(null))) {
      String clusterId = admin.describeCluster().clusterId().get();
      if (Strings.isNullOrEmpty(clusterId)) {
        errors.fatal("Could not connect to Kafka cluster - check configuration");
        return false;
      } else {
        return true;
      }
    } catch (Exception e) {
      errors.fatal("Could not connect to Kafka cluster - check configuration: %s", e);
      return false;
    }
  }

  public String getServersAsString() {
    return String.join(", ", servers);
  }

  private Properties getProperties(String groupId) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", getServersAsString());
    if (!Strings.isNullOrEmpty(groupId)) {
      properties.put("group.id", groupId);
    }
    return properties;
  }

  @Override
  public @NonNull Optional<String> getDefaultName() {
    if (!Strings.isNullOrEmpty(topicPrefix)) {
      //See if we need to truncate suffix
      String name = topicPrefix;
      for (String suffix : TOPIC_SUFFIX) {
        if (name.endsWith(suffix)) {
          name = name.substring(0, name.length() - suffix.length());
          break;
        }
      }
      name = name.trim();
      if (name.length() > 2) {
        return Optional.of(name);
      }
    }
    return Optional.empty();
  }

  @Override
  public Collection<SourceTableConfiguration> discoverTables(
      @NonNull DataSourceConfiguration config, @NonNull ErrorCollector errors) {
    List<SourceTableConfiguration> tables = new ArrayList<>();
    Set<String> topicNames = Collections.EMPTY_SET;
    try (Admin admin = Admin.create(getProperties(null))) {
      topicNames = admin.listTopics().names().get();
    } catch (Exception e) {
      errors.warn("Could not discover Kafka topics: %s", e);
    }
    FormatConfiguration format = config.getFormat();
    NameCanonicalizer canonicalizer = config.getNameCanonicalizer();
    topicNames.stream().filter(n -> n.startsWith(topicPrefix))
        .map(n -> n.substring(topicPrefix.length()).trim())
        .filter(Predicate.not(Strings::isNullOrEmpty))
        .forEach(n -> {
          if (format != null) {
            if (Name.validName(n)) {
              tables.add(new SourceTableConfiguration(n, format));
            } else {
              errors.warn("Topic [%s] has an invalid name and is not added as a table", n);
            }
          } else {
            //try to infer format from topic name
            Pair<String, String> components = FilePath.separateExtension(n);
            FileFormat ff = FileFormat.getFormat(components.getValue());
            if (ff != null && Name.validName(components.getKey())) {
              tables.add(new SourceTableConfiguration(components.getKey(), n,
                  ff.getImplementation().getDefaultConfiguration()));
            }
          }
        });
    return tables;
  }

  @Override
  public boolean update(@NonNull DataSourceConfiguration config, @NonNull ErrorCollector errors) {
    return false;
  }
}
