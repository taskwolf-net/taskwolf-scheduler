package com.dulno.scheduler.trigger;

import com.dulno.core.database.*;
import com.dulno.core.database.condition.DatabaseCondition;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public final class TriggerSchedulerDatabaseTable extends DatabaseTable {
  public static TriggerSchedulerDatabaseTable create(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String tableName,
    List<DatabaseColumn> contentColumns
  ) {
    var columns = Lists.<DatabaseColumn>newArrayList();
    columns.add(DatabaseColumn.create("minute", DatabaseDataType.INT,
      DatabaseColumn.Type.PARTITION_KEY));
    columns.add(DatabaseColumn.create("nextExecution", DatabaseDataType.BIGINT,
      DatabaseColumn.Type.CLUSTERING_KEY));
    columns.add(DatabaseColumn.create("trigger", DatabaseDataType.UUID,
      DatabaseColumn.Type.CLUSTERING_KEY));
    columns.add(DatabaseColumn.create("timezone", DatabaseDataType.TEXT));
    columns.addAll(contentColumns);
    return new TriggerSchedulerDatabaseTable(connection, keyspace, tableName, columns);
  }

  private TriggerSchedulerDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns
  ) {
    super(connection, keyspace, name, columns);
  }

  public void initialize() {
    createIfNotExists();
    createIndexIfNotExists("trigger");
  }

  public CompletableFuture<Void> insertContent(
    UUID triggerId, int minute, long nextExecution, String timezone,
    DatabaseRow content
  ) {
    return this.insert(DatabaseRow.of(new Object[] {minute, nextExecution,
      triggerId, timezone}).concat(content));
  }

  public CompletableFuture<Void> updateContent(
    UUID triggerId, int minute, long nextExecution, String timezone,
    DatabaseRow content
  ) {
    return deleteContent(triggerId).thenCompose(value ->
      insertContent(triggerId, minute, nextExecution, timezone, content));
  }

  public CompletableFuture<Void> deleteContent(UUID triggerId) {
    return findContent(triggerId).thenAccept(content ->
      delete(DatabaseCondition.of("trigger", triggerId, "minute",
        content.findCell(0).integerValue(), "nextExecution",
        content.findCell(1).longValue())));
  }

  public CompletableFuture<DatabaseRow> findContent(UUID triggerId) {
    return selectRow(DatabaseCondition.of("trigger", triggerId));
  }

  public CompletableFuture<List<DatabaseRow>> findContentByCondition(
    DatabaseCondition condition
  ) {
    return selectRows(condition);
  }
}