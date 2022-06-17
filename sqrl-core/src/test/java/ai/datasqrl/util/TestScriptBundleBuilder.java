package ai.datasqrl.util;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.util.data.C360;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.Setter;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestScriptBundleBuilder {

    @NonNull
    private final TestDataset dataset;
    private String scriptContent;
    private boolean includeSchema = false;
    private String version = null;
    private String name = "test";

    public TestScriptBundleBuilder(TestDataset dataset) {
        this.dataset = dataset;
        setScript(ScriptComplexity.LOW);
    }

    public TestScriptBundleBuilder setScript(ScriptComplexity complexity) {
        setScriptContent(dataset.getScriptContent(complexity));
        return this;
    }

    public TestScriptBundleBuilder setScriptContent(String scriptContent) {
        this.scriptContent = scriptContent;
        return this;
    }

    public TestScriptBundleBuilder setIncludeSchema(boolean includeSchema) {
        this.includeSchema = includeSchema;
        return this;
    }

    public TestScriptBundleBuilder setVersion(String version) {
        this.version = version;
        return this;
    }

    public TestScriptBundleBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public ScriptBundle.Config getConfig() {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(scriptContent));
        SqrlScript.Config.ConfigBuilder main = SqrlScript.Config.builder()
                .name(name)
                .main(true)
                .content(scriptContent)
                .filename(name+".sqrl");
        if (includeSchema) {
            Preconditions.checkArgument(dataset.getInputSchema().isPresent(),
                    "Example [%s] does not have an input schema", dataset.getName());
            main.inputSchema(dataset.getInputSchema().get());
        }
        ScriptBundle.Config.ConfigBuilder configBuilder = ScriptBundle.Config.builder()
                .name(name)
                .scripts(ImmutableList.of(main.build()));
        if (version!=null) configBuilder.version(version);
        return configBuilder.build();
    }

    public ScriptBundle getBundle() {
        ScriptBundle.Config config = getConfig();
        ErrorCollector errors = ErrorCollector.root();
        ScriptBundle bundle = config.initialize(errors);
        assertFalse(errors.isFatal(),errors.toString());
        return bundle;
    }



}
