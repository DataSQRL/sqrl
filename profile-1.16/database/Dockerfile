FROM ankane/pgvector:v0.5.0
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=datasqrl

COPY database-schema.sql /docker-entrypoint-initdb.d/init-schema.sql
