package ai.datasqrl.io.impl.kafka;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.DataSourceConnector;
import ai.datasqrl.io.sources.DataSourceConnectorConfig;
import ai.datasqrl.io.sources.DataSourceDiscovery;
import ai.datasqrl.io.sources.DataSourceDiscoveryConfig;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.admin.Admin;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Properties;

@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class KafkaSourceConfig {

    public static final String SOURCE_TYPE = "kafka";

    @NonNull @NotNull @NotEmpty
    List<String> servers;

    String topicPrefix;

    protected boolean rootInitialize(ErrorCollector errors) {
        for (String server : servers) {
            if (Strings.isNullOrEmpty(server)) {
                errors.fatal("Invalid server configuration: %s", server);
            }
        }
        topicPrefix = Strings.isNullOrEmpty(topicPrefix)?"":topicPrefix;

        //Check that we can connect to Kafka cluster
        try (Admin admin = Admin.create(getProperties())) {
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

    public String getSourceType() {
        return SOURCE_TYPE;
    }

    protected String getServersAsString() {
        return String.join(", ", servers);
    }

    protected Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", getServersAsString());
        return properties;
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class Connector extends KafkaSourceConfig implements DataSourceConnectorConfig {

        private Connector(Discovery discovery) {
            super(discovery.servers,discovery.topicPrefix);
        }

        @Override
        public DataSourceConnector initialize(@NonNull ErrorCollector errors) {
            if (rootInitialize(errors)) {
                return new KafkaSource.Connector(getProperties(), topicPrefix);
            } else return null;
        }

    }

    public static class Discovery extends KafkaSourceConfig implements DataSourceDiscoveryConfig {

        @Override
        public DataSourceDiscovery initialize(@NonNull ErrorCollector errors) {
            if (rootInitialize(errors)) {
                return new KafkaSource.Discovery(getProperties(), topicPrefix, new Connector(this));
            } else return null;
        }
    }


}
