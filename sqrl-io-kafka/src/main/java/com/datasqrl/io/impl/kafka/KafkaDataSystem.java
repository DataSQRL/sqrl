package ai.datasqrl.io.impl.kafka;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.util.FileUtil;
import ai.datasqrl.io.formats.FileFormat;
import ai.datasqrl.io.formats.FormatConfiguration;
import ai.datasqrl.io.sources.DataSystemConfig;
import ai.datasqrl.io.sources.DataSystemConnector;
import ai.datasqrl.io.sources.DataSystemDiscovery;
import ai.datasqrl.io.sources.ExternalDataType;
import ai.datasqrl.io.sources.dataset.TableConfig;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.Admin;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;


public abstract class KafkaDataSystem {

  @AllArgsConstructor
  @Getter
  public static class Connector implements DataSystemConnector, Serializable {

    final Properties properties;
    final String topicPrefix;

    @Override
    public boolean hasSourceTimestamp() {
      return true;
    }

    public Properties getProperties(String groupId) {
      Properties copy = new Properties(properties);
      if (!Strings.isNullOrEmpty(groupId)) {
        copy.put("group.id", groupId);
      }
      return copy;
    }

    public String getServersAsString() {
      return properties.getProperty("bootstrap.servers");
    }

  }

  public static class Discovery extends Connector implements DataSystemDiscovery, Serializable {

    public static String[] TOPIC_SUFFIX = {".", "/", "_"};

    final KafkaDataSystemConfig.Connector connectorConfig;

    public Discovery(Properties properties, String topicPrefix, KafkaDataSystemConfig.Connector connectorConfig) {
      super(properties, topicPrefix);
      this.connectorConfig = connectorConfig;
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
    public boolean requiresFormat(ExternalDataType type) {
      if (type.isSource()) return false;
      else return true;
    }


    @Override
    public Collection<TableConfig> discoverSources(
            @NonNull DataSystemConfig config, @NonNull ErrorCollector errors) {
      List<TableConfig> tables = new ArrayList<>();
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
              .forEach(name -> {
                TableConfig.TableConfigBuilder tblBuilder = TableConfig.copy(config);
                tblBuilder.connector(connectorConfig);

                if (format != null) {
                  if (Name.validName(name)) {
                    tblBuilder.identifier(name).name(name);
                    tblBuilder.format(format);
                  } else {
                    errors.warn("Topic [%s] has an invalid name and is not added as a table", name);
                  }
                } else {
                  //try to infer format from topic name
                  Pair<String, String> components = FileUtil.separateExtension(name);
                  FileFormat ff = FileFormat.getFormat(components.getValue());
                  if (ff != null && Name.validName(components.getKey())) {
                    tblBuilder.identifier(name).name(components.getKey());
                    tblBuilder.format(ff.getImplementation().getDefaultConfiguration());
                  } else {
                    errors.warn("Could not infer format for topic [%s] and is not added as a table", name);
                  }
                  tables.add(tblBuilder.build());
                }
              });
      return tables;
    }

  }



}
