package ai.dataeng.sqml.config.scripts;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.config.constraints.OptionalMinString;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.schema.external.SchemaDefinition;
import lombok.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Value
public class SqrlScript {

    private final Name name;
    private final String scriptContent;
    private final SchemaDefinition schema;
    private boolean isMain;


    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Config {

        @NonNull @NotNull @Size(min = 3, max = 128)
        private String name;
        @NonNull @NotNull @Size(min = 10)
        private String scriptContent;

        @OptionalMinString
        private String schemaYAML;
        @Builder.Default
        private boolean isMain = false;

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
            return new SqrlScript(Name.of(name,canonicalizer),scriptContent,schema,isMain);
        }

        private SchemaDefinition parseSchema() throws JsonProcessingException {
            if (StringUtils.isEmpty(schemaYAML)) {
                return SchemaDefinition.empty();
            } else {
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                SchemaDefinition importSchema = mapper.readValue(schemaYAML,
                        SchemaDefinition.class);
                return importSchema;
            }
        }

    }



}
