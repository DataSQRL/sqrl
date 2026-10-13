/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.jetbrains.intellij.platform.gradle.IntelliJPlatformType
import org.jetbrains.intellij.platform.gradle.TestFrameworkType
import org.jetbrains.intellij.platform.gradle.tasks.PrepareSandboxTask

plugins {
    java
    id("org.jetbrains.intellij.platform") version "2.19.0"
}

// The release of the CLI that the plugin is built for, e.g. -PsqrlVersion=1.0.0: it is the version
// of the plugin, which starts the Docker image of that release by default
val sqrlVersion = providers.gradleProperty("sqrlVersion")

group = "com.datasqrl"
version = sqrlVersion.getOrElse("0.1.0")

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
    intellijPlatform {
        defaultRepositories()
    }
}

dependencies {
    intellijPlatform {
        // The first version with the current LSP API, which plugin.xml requires
        intellijIdea("2026.1.4")
        bundledPlugin("org.jetbrains.plugins.textmate")
        bundledPlugin("com.intellij.modules.json")
        testFramework(TestFrameworkType.Platform)
    }
    testImplementation("junit:junit:4.13.2")
}

intellijPlatform {
    pluginConfiguration {
        ideaVersion {
            sinceBuild = "261.26222"
        }
    }
    pluginVerification {
        ides {
            // The first version with the current LSP API, and the latest
            create(IntelliJPlatformType.IntellijIdea, "2026.1.4")
            create(IntelliJPlatformType.IntellijIdea, "2026.2.3")
        }
    }
}

tasks {
    test {
        // The end-to-end test starts the language server from the jar of the CLI, if it was built
        systemProperty(
            "sqrl.cli.jar",
            layout.projectDirectory.file("../sqrl-cli/target/sqrl-cli.jar").asFile.absolutePath,
        )
    }

    // Package files are edited with the JSON schema that the compiler validates them with
    processResources {
        // A development build starts the image of the latest release
        val image = "datasqrl/cmd:" + sqrlVersion.getOrElse("latest")
        inputs.property("image", image)
        filteringCharset = "UTF-8"
        filesMatching("com/datasqrl/intellij/build.properties") {
            expand("image" to image)
        }
        from(layout.projectDirectory.file("../sqrl-planner/src/main/resources/jsonSchema/packageSchema.json")) {
            into("jsonSchema")
        }
    }

    // TextMate bundles are read from the file system, so the grammar is next to the jars of the
    // plugin, also in the sandboxes of tests
    withType<PrepareSandboxTask>().configureEach {
        from(layout.projectDirectory.dir("src/main/textmate")) {
            into(pluginName.map { "$it/textmate" })
        }
    }
}
