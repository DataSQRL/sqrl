#
# Copyright © 2021 DataSQRL (contact@datasqrl.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ARG BASE_TAG
FROM datasqrl/base-sqrl:${BASE_TAG}

COPY sqrl-cli/target/sqrl-cli.jar /opt/sqrl/sqrl-cli.jar
COPY sqrl-run/target/sqrl-run.jar /opt/sqrl/sqrl-run.jar
COPY sqrl-test/target/sqrl-test.jar /opt/sqrl/sqrl-test.jar

COPY dockerrun.sh /opt/sqrl/dockerrun.sh
COPY redpanda.yaml /etc/redpanda/redpanda.yaml

VOLUME /data/redpanda

env JAVA_TOOL_OPTIONS="--add-exports=java.base/sun.net.util=ALL-UNNAMED \
  --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED \
  --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED \
  --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
  --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED \
  --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED \
  --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.text=ALL-UNNAMED \
  --add-opens=java.base/java.time=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED"


ENTRYPOINT ["/opt/sqrl/dockerrun.sh"]
