/**
 * The GraphQL package is used to execute GraphQL queries against a host database. It provides
 * different providers to interpret different GraphQL nodes, including paging providers and argument
 * providers. It also includes a schema provider to fetch a schema from the SQRL bundle, and a query
 * executor to execute queries against a schema.
 * <p>
 * The philosophy of the GraphQL package is to be as un-opinionated as possible. This means that it
 * should be easy to create and customize APIs to fit your needs.
 * <p>
 * To provide this, the GraphQL package includes features like the ability to generate a full
 * GraphQL schema, prune an existing schema, the ability to create new queries, the ability to
 * change data types, support for GraphQL inheritance models, and support for implementing custom
 * argument and paging providers.
 */
package ai.datasqrl.graphql;
