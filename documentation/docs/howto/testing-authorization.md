# Testing Authorization

You can test record filtering, data masking, and other types of authorization based data access control with DataSQRL's automated test runner via the [`test` command](../compiler#test-command).

## Generating Tokens

To generate test tokens, one of the most straightforward ways would be to use the **JWT Encoder** functionality of https://jwt.io.
The **Payload: Data** can be shaped to our testing needs.
For testing purposes, using the `HS256` algorithm probably should be completely fine.
Then, we only need to define a long enough signer secret string on the website.

The only manual step that is required in case of `HS265` is to apply Base64 encoding to your given secret, for example:
```sh
echo -n mySuperSecretSignerStringThatIsLongEnough | base64
```

And then we need to set the encoded secret as the `buffer` in the `package.json` file `vertx` config section:

```json
{
  ...
  "engines" : {
    "vertx" : {
      "enabledAuth": ["JWT"],
      "config": {
        "jwtAuth": {
          "pubSecKeys": [
            {
              "algorithm": "HS256",
              "buffer": "bXlTdXBlclNlY3JldFNpZ25lclN0cmluZ1RoYXRJc0xvbmdFbm91Z2gK"
            }
          ],
          ...
        }
      }
    }
  },
  ...
}
```

## Default Test Runner Token

We can set one token directly to the `test-runner` configuration that the deployed test server will pick up by default.
Any valid HTTP headers can be defined in `headers` if necessary, but in this context the important one is `Authorization`.
These headers will be added to any request that will be executed during the test.

```json
{
  "test-runner": {
    "headers": {
      "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJteS10ZXN0LWlzc3VlciIsImF1ZCI6WyJteS10ZXN0LWF1ZGllbmNlIl0sImV4cCI6OTk5OTk5OTk5OSwidmFsIjoxfQ.cvgte5Lfhrsr2OPoRM9ecJbxehBQzwHaghANY6MvhqE"
    }
  }
}
```

## Additional Tests

To be able to test different scenarios, it is mandatory to be able to provide different tokens that simulate them.
To achieve this, we can define any new test case under the project's `test-folder`, the test execution will pick them up and also compare it with their respective snapshots.
A custom JWT test case will require two files, which share the same name that will function as the name of the test case:
* A `.graphql` file that should define a query, mutation, or subscription.
* A `.properties` file if the test case requires a different token than the one defined in `test-runner

A sample test case structure with three different test cases looks like the below file tree.

```
├── tests/
│   ├── mutationWithSameToken.graphql
│   ├── subscriptionWithSameToken.graphql
│   ├── differentUserQuery.graphql
│   ├── differentUserQuery.properties
│   ...
```

The content of the `.properties` override the applied `headers` tor the matching `.graphql` requests, making it possible to define different scenarios.
A simple JWT override header properties file would look like this:

```properties
Authorization: Bearer <test-specific-jwt>
```
