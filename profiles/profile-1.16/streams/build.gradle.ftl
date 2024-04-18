plugins {
    id 'java'
    id 'com.github.johnrengelman.shadow' version '8.1.1'
}

group 'com.example'
version '1.0-SNAPSHOT'

sourceCompatibility = '11'

ext {
    flinkVersion = "1.16.1"
    jdbcVersion = "1.16.1"
    kafkaVersion = "1.16.1"
    sqrlVersion = "0.5.0-RC2-SNAPSHOT"
    icebergVersion = "1.5.0"
}

repositories {
    mavenLocal()
    mavenCentral()
    maven {
        url "https://packages.confluent.io/maven/"
    }
}

dependencies {
    compileOnly "org.apache.flink:flink-java:$flinkVersion"
    compileOnly "org.apache.flink:flink-streaming-java:$flinkVersion"
<#if plan.get("streams").connectors?seq_contains("kafka") || plan.get("streams").connectors?seq_contains("upsert-kafka")>
    implementation "org.apache.flink:flink-connector-kafka:$kafkaVersion"
</#if>
<#if plan.get("streams").connectors?seq_contains("jdbc") || plan.get("streams").connectors?seq_contains("jdbc-sqrl")>
    implementation "org.apache.flink:flink-connector-jdbc:$jdbcVersion"
</#if>
<#if plan.get("streams").connectors?seq_contains("iceberg")>
    implementation "org.apache.iceberg:iceberg-core:$icebergVersion"
    implementation "org.apache.iceberg:iceberg-aws:$icebergVersion"
    implementation "org.apache.iceberg:iceberg-flink-$flinkBaseVersion:$icebergVersion"
    implementation "org.apache.iceberg:iceberg-flink-runtime-$flinkBaseVersion:$icebergVersion"
</#if>
    compileOnly "org.apache.flink:flink-table-api-java:$flinkVersion"
    compileOnly "org.apache.flink:flink-table-runtime:$flinkVersion"
    compileOnly "org.apache.flink:flink-table-api-java-bridge:$flinkVersion"
<#if plan.get("streams").connectors?seq_contains("filesystem")>
    compileOnly "org.apache.flink:flink-connector-files:$flinkVersion"
</#if>
<#if plan.get("streams").formats?seq_contains("avro")>
    implementation "org.apache.flink:flink-avro:$flinkVersion"
    implementation "org.apache.flink:flink-avro-confluent-registry:$flinkVersion"
</#if>
    implementation "run.sqrl:sqrl-lib-common:$sqrlVersion"
    implementation "run.sqrl:sqrl-json:$sqrlVersion"
    implementation "run.sqrl:sqrl-secure:$sqrlVersion"
    implementation "run.sqrl:sqrl-time:$sqrlVersion"
    implementation "run.sqrl:sqrl-text:$sqrlVersion"
    implementation "run.sqrl:sqrl-flexible-json:$sqrlVersion"
    implementation "run.sqrl:sqrl-flexible-csv:$sqrlVersion"
    implementation "run.sqrl:sqrl-vector:$sqrlVersion"
<#if plan.get("streams").formats?seq_contains("jdbc-sqrl") || plan.get("streams").connectors?seq_contains("jdbc-sqrl")>
    implementation "run.sqrl:sqrl-jdbc-1.16:$sqrlVersion"
</#if>

    // Log4j 2 dependencies
    implementation 'org.apache.logging.log4j:log4j-core:2.17.1'
    implementation 'org.apache.logging.log4j:log4j-api:2.17.1'
    implementation 'org.apache.logging.log4j:log4j-slf4j-impl:2.17.1'

    testImplementation "org.apache.flink:flink-connector-files:$flinkVersion"
    testImplementation "org.apache.flink:flink-json:$flinkVersion"

    testImplementation platform('org.junit:junit-bom:5.9.1')
    testImplementation 'org.junit.jupiter:junit-jupiter'

    testImplementation "org.apache.flink:flink-table-planner_2.12:$flinkVersion"
    testImplementation "org.apache.flink:flink-runtime-web:$flinkVersion"
}

test {
    useJUnitPlatform()
}

task copyDependencies(type: Copy) {
    from configurations.runtimeClasspath
    into 'build/dependencies'
}

shadowJar {
    mergeServiceFiles()
    manifest {
        attributes 'Main-Class': 'com.datasqrl.FlinkMain'
    }
    from('.') {
        include 'lib/**'
        include 'flink-plan.sql'
        include 'flink-conf.yaml'
        into('.')
    }
    zip64 = true
    archiveFileName = 'FlinkJob.jar'
}