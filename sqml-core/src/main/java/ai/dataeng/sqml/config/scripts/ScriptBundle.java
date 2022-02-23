package ai.dataeng.sqml.config.scripts;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.config.util.ConfigurationUtil;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;

import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An {@link ScriptBundle} contains the main SQML script that defines the dataset to be exposed as an API as well
 * as all supporting SQML scripts that are imported (directly or indirectly) by the main script.
 *
 * In addition, the bundle may include an optional schema file that defines the schema of the input data, API, and can
 * provide additional hints that guide the optimizer on how to generate the denormalizations.
 *
 * Production {@link ScriptBundle} must also contain the queries that get deployed in the API.
 */

@Value
public class ScriptBundle {

    public static final NameCanonicalizer CANONICALIZER = NameCanonicalizer.SYSTEM;

    private final Name name;
    private final Map<Name, SqrlScript> scripts;
    private final Map<Name,SqrlQuery> queries;

    public SqrlScript getMainScript() {
        if (scripts.size()==1) return scripts.values().iterator().next();
        else return scripts.values().stream().filter(SqrlScript::isMain).findFirst().get();
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Config {

        @NonNull @NotNull @Size(min = 3, max=128)
        private String name;
        @NonNull @NotNull @NotEmpty @Valid
        private List<SqrlScript.Config> scripts;
        @NonNull @Builder.Default @NotNull @Valid
        private List<SqrlQuery.Config> queries = new ArrayList<>();

        public ScriptBundle initialize(ProcessMessage.ProcessBundle<ConfigurationError> errors) {
            if (!ConfigurationUtil.javaxValidate(this, errors)) return null;

            List<SqrlScript> validScripts = scripts.stream().map(s -> s.initialize(errors, name, CANONICALIZER))
                    .filter(Objects::nonNull).collect(Collectors.toList());

            List<SqrlQuery> validQueries = queries.stream().map(s -> s.initialize(errors, name, CANONICALIZER))
                    .filter(Objects::nonNull).collect(Collectors.toList());

            //See if we encountered any errors
            if (validScripts.size()!=scripts.size() || validQueries.size()!=queries.size()) return null;

            if (validScripts.isEmpty()) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT,name,
                        "Need to define at least one script in bundle"));
            }

            boolean isvalid = true;
            List<Name> duplicates =
            validScripts.stream().collect(Collectors.groupingBy(SqrlScript::getName, Collectors.counting()))
                            .entrySet().stream().filter(e -> e.getValue() > 1).map(Map.Entry::getKey).collect(Collectors.toList());
            if (!duplicates.isEmpty()) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT, name,
                        "Script names must be unique within a bundle, but found the following duplicates: [%s]",duplicates));
                isvalid = false;
            }

            duplicates =
                    validQueries.stream().collect(Collectors.groupingBy(SqrlQuery::getName, Collectors.counting()))
                            .entrySet().stream().filter(e -> e.getValue() > 1).map(Map.Entry::getKey).collect(Collectors.toList());
            if (!duplicates.isEmpty()) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT, name,
                        "Query names must be unique within a bundle, but found the following duplicates: [%s]",duplicates));
                isvalid = false;
            }

            if (validScripts.size()>1) {
                List<Name> mainScripts = validScripts.stream().filter(SqrlScript::isMain).map(SqrlScript::getName).collect(Collectors.toList());
                if (mainScripts.isEmpty()) {
                    errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT, name,
                            "Need to set one script as `main` when there are multiple scripts in the bundle"));
                    isvalid = false;
                } else if (mainScripts.size()>1) {
                    errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT, name,
                            "Only one script can be set as `main`, but found the following main scripts: [%s]", mainScripts));
                    isvalid = false;
                }
            }

            if (isvalid) {
                return new ScriptBundle(Name.of(name,CANONICALIZER),
                        validScripts.stream().collect(Collectors.toMap(SqrlScript::getName, Function.identity())),
                        validQueries.stream().collect(Collectors.toMap(SqrlQuery::getName, Function.identity()))
                );
            } else {
                return null;
            }
        }


    }



}
