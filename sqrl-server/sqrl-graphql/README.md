# Acorn GraphQL

This module contains the core implementation classes and interfaces of Acorn. This base module is used by framework specific modules and can be used as the basis for specific implementations for frameworks or LLMs.

Specific implementation that utilize this module need to implement two things:
1. An implementation of `APIQueryExecutor` for executing GraphQL queries
2. Passing the generated `APIFunction` to the LLM and validating/executing them when invoked by the LLM. This usually requires a thin wrapper based on the LLM framework or SDK used.

This module consists of the following packages:

* [Tool](src/main/java/com/datasqrl/ai/tool): Defines the `APIFunction` class which represents and LLM tool and how it maps to GraphQL API queries. Also defines `Context` for sensitive information sandboxing.
* [Converter](src/main/java/com/datasqrl/ai/converter): Converts a provided GraphQL schema or individual GraphQL operations to `APIFunction`.
* [API](src/main/java/com/datasqrl/ai/api): Interfaces and methods for API invocation.
* [Chat](src/main/java/com/datasqrl/ai/chat): Saving and retrieving messages from GraphQL API.
* [Util](src/main/java/com/datasqrl/ai/util): Utility classes/methods used across packages.

