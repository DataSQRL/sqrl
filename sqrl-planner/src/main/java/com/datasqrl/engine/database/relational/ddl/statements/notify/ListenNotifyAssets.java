package com.datasqrl.engine.database.relational.ddl.statements.notify;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ListenNotifyAssets {
  ListenQuery listen;
  OnNotifyQuery onNotify;
  List<String> parameters;
}
