package com.datasqrl.engine.database.relational.ddl.statements.notify;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ListenNotifyAssets {
  ListenQuery listen;
  OnNotifyQuery onNotify;
}
