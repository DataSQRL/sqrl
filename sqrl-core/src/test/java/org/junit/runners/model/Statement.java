package org.junit.runners.model;
/*
* testcontainers has a hard dependency on JUnit4, which we would like to avoid, migrating to
* JUnit5 (aka junit jupiter). Until testcontainers updates their package to remove this dependency,
* a workaround is needed to avoid the "org.junit.rules.TestRule not found" error. The two files
* Statement.java and TestRule.java seem to accomplish this, following a comment in the github thread
* where multiple people are encountering the same problem:
* https://github.com/testcontainers/testcontainers-java/issues/970#issuecomment-625044008
 */
@SuppressWarnings("unused")
public class Statement {
}