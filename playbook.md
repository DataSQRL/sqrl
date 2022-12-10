## Getting started with development
First run `mvn clean install` and let all the tests run. This is
required for development since it'll install the relevant test
and dev poms to your local .m2 folder. Run clean install whenever
you make dependency changes and are trying to do a maven goal inside
a maven subdirectory.

## How to build Parser in Intellij

1. Run sqrl-parser > Plugins > antlr4:antlr4
2. Refresh maven dependencies

## Helpful commands while developing

`mvn -Dmaven.test.skip compile jar:test-jar install`