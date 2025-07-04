package com.datasqrl.graphql.server;

public enum MutationInsertType {
  SINGLE, // The mutation is for single inserts (default)
  BATCH, // The mutation takes an array of records to insert in a batch (without guarantees)
  TRANSACTION; // The mutation takes an array of records to insert as one atomic transaction

  public boolean isMultiple() {
    return this != SINGLE;
  }
}
