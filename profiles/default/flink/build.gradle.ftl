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
    sqrlVersion = "${config["compile"]["sqrl-version"]}"
    icebergVersion = "1.5.2"
<#if config["enabled-engines"]?seq_contains("postgres_log")>
    postgresCdcVersion = "3.1.0"
</#if>
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
    compileOnly "org.apache.flink:flink-table-api-java-bridge:$flinkVersion"

    implementation "org.apache.flink:flink-connector-kafka:$kafkaVersion"
    implementation "org.apache.flink:flink-connector-jdbc:$jdbcVersion"
    implementation "org.apache.flink:flink-avro-confluent-registry:$flinkVersion"
<#if config["enabled-engines"]?seq_contains("postgres_log")>
    implementation "org.apache.flink:flink-connector-postgres-cdc:$postgresCdcVersion"
</#if>

    implementation "com.datasqrl:sqrl-lib-common:$sqrlVersion"
    implementation "com.datasqrl:sqrl-json:$sqrlVersion"
    implementation "com.datasqrl:sqrl-secure:$sqrlVersion"
    implementation "com.datasqrl:sqrl-time:$sqrlVersion"
    implementation "com.datasqrl:sqrl-text:$sqrlVersion"
    implementation "com.datasqrl:sqrl-flexible-json:$sqrlVersion"
    implementation "com.datasqrl:sqrl-flexible-csv:$sqrlVersion"
    implementation "com.datasqrl:sqrl-vector:$sqrlVersion"
    implementation "com.datasqrl:sqrl-jdbc-1.18:$sqrlVersion"

    implementation "org.apache.hive:hive-metastore:3.1.3"
    implementation 'org.apache.iceberg:iceberg-flink-runtime-1.18:1.5.2'
    //implementation "org.apache.iceberg:iceberg-core:$icebergVersion"
    //implementation "org.apache.iceberg:iceberg-aws:$icebergVersion"

    implementation platform('software.amazon.awssdk:bom:2.24.5')
    implementation "software.amazon.awssdk:apache-client"
    implementation "software.amazon.awssdk:auth"
    implementation "software.amazon.awssdk:iam"
    implementation "software.amazon.awssdk:sso"
    implementation "software.amazon.awssdk:s3"
    implementation "software.amazon.awssdk:kms"
    implementation "software.amazon.awssdk:glue"
    implementation "software.amazon.awssdk:sts"
    implementation "software.amazon.awssdk:dynamodb"
    implementation "software.amazon.awssdk:lakeformation"

    // Log4j 2 dependencies
//    testImplementation 'org.apache.logging.log4j:log4j-core:2.17.1'
//    testImplementation 'org.apache.logging.log4j:log4j-api:2.17.1'
//    testImplementation 'org.apache.logging.log4j:log4j-slf4j-impl:2.17.1'
//
//    testImplementation "org.apache.flink:flink-table-api-java:$flinkVersion"
//    testImplementation "org.apache.flink:flink-table-runtime:$flinkVersion"
//    testImplementation "org.apache.flink:flink-connector-files:$flinkVersion"
//    testImplementation "org.apache.flink:flink-json:$flinkVersion"
//
//    testImplementation platform('org.junit:junit-bom:5.9.1')
//    testImplementation 'org.junit.jupiter:junit-jupiter'
//
//    testImplementation "org.apache.flink:flink-statebackend-rocksdb:$flinkVersion"
//    testImplementation "org.apache.flink:flink-table-planner_2.12:$flinkVersion"
//    testImplementation "org.apache.flink:flink-runtime-web:$flinkVersion"
    testRuntimeOnly fileTree(dir: 'lib', include: '*.jar')
}
//
//test {
//    minHeapSize = "128m" // initial heap size
//    maxHeapSize = "2g" // maximum heap size
//    jvmArgs '-XX:MaxPermSize=256m' // mem argument for the test JVM
//}
//
//test {
//    useJUnitPlatform()
//}
//
//task copyDependencies(type: Copy) {
//    from configurations.runtimeClasspath
//    into 'build/dependencies'
//}

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