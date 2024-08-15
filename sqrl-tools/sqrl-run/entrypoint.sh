#!/bin/bash
set -e

# Keep the container running if needed (useful for debugging; remove in production)
# tail -f /dev/null
service postgresql start

echo "Starting services..."
while ! pg_isready -q; do
    sleep 1
done

java -jar /app/sqrl-run.jar run ${@}
