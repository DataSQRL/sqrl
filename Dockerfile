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

# Use Gradle 8.6 with JDK 11 as the base image
FROM gradle:8.6-jdk17 AS build

# Update the package list and install the 'patch' utility
RUN apt-get update && apt-get install -y maven patch

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app/

ENV TZ=America/Los_Angeles

# Run Maven clean and install commands, skipping tests
RUN mvn clean install -DskipTests
