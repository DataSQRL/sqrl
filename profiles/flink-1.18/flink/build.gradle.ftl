plugins {
    id 'java'
    id 'com.github.johnrengelman.shadow' version '8.1.1'
}

group 'com.example'
version '1.0-SNAPSHOT'

sourceCompatibility = '11'

ext {
    flinkVersion = "1.18.1"
    jdbcVersion = "3.1.2-1.18"
    kafkaVersion = "3.1.0-1.18"
    sqrlVersion = "0.5.0-RC2-SNAPSHOT"
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
    implementation "org.apache.flink:flink-connector-kafka:$kafkaVersion"
    implementation "org.apache.flink:flink-connector-jdbc:$jdbcVersion"
    compileOnly "org.apache.flink:flink-table-api-java:$flinkVersion"
    compileOnly "org.apache.flink:flink-table-runtime:$flinkVersion"
    compileOnly "org.apache.flink:flink-table-api-java-bridge:$flinkVersion"
    compileOnly "org.apache.flink:flink-connector-files:$flinkVersion"
    implementation "org.apache.flink:flink-avro:$flinkVersion"
    implementation "org.apache.flink:flink-avro-confluent-registry:$flinkVersion"

    implementation "run.sqrl:sqrl-lib-common:$sqrlVersion"
    implementation "run.sqrl:sqrl-json:$sqrlVersion"
    implementation "run.sqrl:sqrl-secure:$sqrlVersion"
    implementation "run.sqrl:sqrl-time:$sqrlVersion"
    implementation "run.sqrl:sqrl-text:$sqrlVersion"
    implementation "run.sqrl:sqrl-flexible-json:$sqrlVersion"
    implementation "run.sqrl:sqrl-vector:$sqrlVersion"
//<#if flink["formats"]?seq_contains("jdbc-sqrl") || flink["connectors"]?seq_contains("jdbc-sqrl")>
    implementation "run.sqrl:sqrl-jdbc-1.18:$sqrlVersion"
//</#if>

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
        into('.')
    }
    zip64 = true
    archiveFileName = 'FlinkJob.jar'
}