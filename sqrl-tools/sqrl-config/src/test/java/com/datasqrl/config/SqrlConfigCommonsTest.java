package com.datasqrl.config;

import static org.assertj.core.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;

class SqrlConfigCommonsTest {

    private ErrorCollector errors;

    @BeforeEach
    void setup() {
        errors = new ErrorCollector(ErrorPrefix.ROOT);
    }

    @Test
    void givenNoPaths_whenCreatingConfig_thenReturnDefaults() {
        PackageJson underTest = SqrlConfigCommons.fromFilesPackageJson(errors, List.of());

        assertThat(underTest).isNotNull();
        assertThat(underTest.getVersion()).isEqualTo(1);
        assertThat(underTest.getEnabledEngines()).contains("vertx", "postgres", "kafka", "flink");
        assertThat(underTest.getTestConfig()).isPresent();
        assertThat(underTest.getEngines().getEngineConfig("flink")).isPresent();
        assertThat(underTest.getScriptConfig().getGraphql()).isEmpty();
        assertThat(underTest.getScriptConfig().getMainScript()).isEmpty();
        assertThat(underTest.getPackageConfig().getName()).isEqualTo("datasqrl.profile.default");
    }

    @Test
    void givenSinglePath_whenCreatingConfig_thenOverrideDefaults() {
        PackageJson underTest = SqrlConfigCommons.fromFilesPackageJson(errors, List.of(Path.of("src/test/resources/config/test-package.json")));

        assertThat(underTest).isNotNull();
        assertThat(underTest.getVersion()).isEqualTo(1);

        //test-package.json overrides enabled engines ONLY
        assertThat(underTest.getEnabledEngines()).contains("test");

        assertThat(underTest.getTestConfig()).isPresent();
        assertThat(underTest.getEngines().getEngineConfig("flink")).isPresent();
        assertThat(underTest.getScriptConfig().getGraphql()).isEmpty();
        assertThat(underTest.getScriptConfig().getMainScript()).isEmpty();
        assertThat(underTest.getPackageConfig().getName()).isEqualTo("datasqrl.test");
    }

}
