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
package com.datasqrl.container.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class HttpLookupConnectorContainerIT extends SqrlContainerTestBase {

  private static final String HTTP_LOOKUP_MOCK_ALIAS = "http-lookup-mock";
  private static final int HTTP_LOOKUP_MOCK_PORT = 18080;

  private GenericContainer<?> httpLookupMock;

  @Override
  protected String getTestCaseName() {
    return "http-lookup";
  }

  @Test
  @SneakyThrows
  void givenSqrlWithHttpLookup_whenRun_thenHttpLookupInvokesMockServer() {
    startHttpLookupMock();

    cmd =
        createCmdContainer(testDir)
            .withNetwork(sharedNetwork)
            .withCommand("test", "package.json")
            .withExposedPorts(HTTP_SERVER_PORT)
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(HTTP_SERVER_PORT)
                    .forStatusCode(204)
                    .withStartupTimeout(Duration.ofMinutes(3)));

    cmd.start();

    // Wait for test(s) to complete
    await().atMost(Duration.ofMinutes(5)).until(() -> !cmd.isRunning());

    var logs = cmd.getLogs();
    assertThat(logs)
        .contains("Running Tests")
        .contains("EnrichedOrdersTest")
        .contains("BUILD SUCCESS");
  }

  private void startHttpLookupMock() {
    // Small Python HTTP server that echoes join keys back as JSON.
    // The SQRL script points the connector at: http://http-lookup-mock:18080/client
    var python =
        """
        import json
        from http.server import BaseHTTPRequestHandler, HTTPServer
        from urllib.parse import urlparse, parse_qs

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                # Silence default access logs; the test uses our explicit print.
                pass

            def do_GET(self):
                p = urlparse(self.path)
                if p.path != '/client':
                    self.send_response(404)
                    self.end_headers()
                    return

                q = parse_qs(p.query)
                id = (q.get('id') or [''])[0]
                id2 = (q.get('id2') or [''])[0]

                print(f'GET /client {p.query}', flush=True)

                body = json.dumps({
                    'id': id,
                    'id2': id2,
                    'msg': 'hello',
                    'uuid': '00000000-0000-0000-0000-000000000000'
                })

                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(body.encode('utf-8'))

        HTTPServer(('0.0.0.0', %d), Handler).serve_forever()
        """
            .formatted(HTTP_LOOKUP_MOCK_PORT);

    httpLookupMock =
        new GenericContainer<>(DockerImageName.parse("python:3.12-alpine"))
            .withNetwork(sharedNetwork)
            .withNetworkAliases(HTTP_LOOKUP_MOCK_ALIAS)
            .withExposedPorts(HTTP_LOOKUP_MOCK_PORT)
            .withCommand("python", "-u", "-c", python)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)));

    httpLookupMock.start();
  }

  @Override
  protected void cleanupContainers() {
    super.cleanupContainers();

    if (httpLookupMock != null && httpLookupMock.isRunning()) {
      httpLookupMock.stop();
      httpLookupMock = null;
    }
  }
}
