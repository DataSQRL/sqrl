package ai.dataeng.sqml.config.scripts;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.config.constraints.OptionalMinString;
import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.schema.external.SchemaDefinition;
import java.io.Serializable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

@Value
public class SqrlScript implements Serializable {

    private final Name name;
    private final String filename;
    private final String content;
    private final SchemaDefinition schema;
    private boolean main;


    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Config {

        @NonNull @NotNull @Size(min = 3, max = 128)
        private String name;
        private String filename;
        @NonNull @NotNull @Size(min = 10)
        private String content;

        @OptionalMinString
        private String inputSchema;
        @Builder.Default
        private boolean main = false;

        SqrlScript initialize(ProcessMessage.ProcessBundle<ConfigurationError> errors,
                              String bundleName, NameCanonicalizer canonicalizer) {
            if (!ConfigurationUtil.javaxValidate(this, errors)) return null;
            SchemaDefinition schema;
            try {
                 schema = parseSchema();
            } catch (JsonProcessingException e) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT,
                        String.join(".",bundleName,name),
                        "Parsing error for schemaYaml: [%s]", e));
                return null;
            }
            return new SqrlScript(Name.of(name,canonicalizer),
                    StringUtils.isNotEmpty(filename)?filename:name,
                    content,schema, main);
        }

        private SchemaDefinition parseSchema() throws JsonProcessingException {
            if (StringUtils.isEmpty(inputSchema)) {
                return SchemaDefinition.empty();
            } else {
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                SchemaDefinition importSchema = mapper.readValue(inputSchema,
                        SchemaDefinition.class);
                return importSchema;
            }
        }

    }



}
