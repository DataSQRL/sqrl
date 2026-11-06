## Authentication Configuration

Configure authentication in the `vertx` engine with the `auth` block: the list of trusted token issuers, each with its
own audiences and signing keys. A token is accepted only when its `iss`, `aud` and signature all match the same issuer.
The full reference, including the deprecated `jwtAuth` and `oauthConfig` blocks, is in the `/configure-sqrl` skill
([vertx.md](../configure-sqrl/vertx.md#authentication)).

## Testing Authorization

You can test record filtering, data masking, and other types of authorization based data access control with DataSQRL's automated test runner via the `test` command (see the `/test-sqrl` skill).

### Generating Tokens

Test tokens are generated with the container's built-in **`jwt`** helper — a
small, dependency-free CLI available on `PATH` inside the agent. For testing, the
`HS256` (shared-secret) algorithm is simplest and is what these examples use.

A single secret string is the signing key, and it relates to the config two ways:

- **`--secret`** passed to `jwt encode` is the **raw** secret string.
- **`k`** of an `oct` key in `auth` is that same secret, **base64url-encoded**
  without padding. Encode it with `printf` — not `echo`, which appends a trailing
  newline that changes the key and makes every token fail to validate — and strip
  the line breaks `base64` inserts into long output:

```sh
printf '%s' 'mySuperSecretSignerStringThatIsLongEnough' | base64 | tr '+/' '-_' | tr -d '=\n'
# -> bXlTdXBlclNlY3JldFNpZ25lclN0cmluZ1RoYXRJc0xvbmdFbm91Z2g
```

Set it as the `k` of an `oct` key, which verifies `HS256` tokens:

```json
"auth": {
  "issuers": [
    {
      "issuer": "<jwt-issuer>",
      "audiences": ["<jwt-audience>"],
      "keys": [{ "kty": "oct", "k": "bXlTdXBlclNlY3JldFNpZ25lclN0cmluZ1RoYXRJc0xvbmdFbm91Z2g" }]
    }
  ]
}
```

Then mint a token by passing the **raw** secret and the claims (shaped to your
test's needs; `iss` / `aud` must match an `auth.issuers` entry, or the deprecated `jwtOptions`):

```sh
jwt encode --secret 'mySuperSecretSignerStringThatIsLongEnough' --alg HS256 \
  '{"iss": "<jwt-issuer>", "aud": ["<jwt-audience>"], "exp": 9999999999, "customerId": 6, "roles": ["user"]}'
```

Inspect or verify an existing token with `jwt decode` (pass `--secret` to also
check the signature):

```sh
jwt decode --secret 'mySuperSecretSignerStringThatIsLongEnough' <token>
```

### Default Test Runner Token

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

### Test Specific Token

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
