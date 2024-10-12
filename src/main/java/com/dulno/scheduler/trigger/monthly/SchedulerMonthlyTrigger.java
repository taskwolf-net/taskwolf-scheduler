package com.dulno.scheduler.trigger.monthly;

import com.dulno.core.database.*;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.trigger.Trigger;
import com.dulno.core.trigger.TriggerContentDatabaseTable;
import com.dulno.core.trigger.TriggerInformation;
import com.dulno.core.workflow.component.input.InputComponentDataType;
import com.dulno.core.workflow.component.input.InputComponentVariable;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public final class SchedulerMonthlyTrigger implements Trigger {
  public static SchedulerMonthlyTrigger create(
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("day", DatabaseDataType.INT));
    contentColumns.add(DatabaseColumn.create("time", DatabaseDataType.TEXT));
    return new SchedulerMonthlyTrigger(
      TriggerContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "trigger_scheduler_monthly", contentColumns));
  }

  private final TriggerContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "scheduler-monthly-trigger";
  }

  @Override
  public TriggerInformation information() {
    return TriggerInformation.builder()
      .withName("scheduler.trigger.monthly.name")
      .withDescription("scheduler.trigger.monthly.description")
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.monthly.input.day.name",
        "day", "scheduler.trigger.monthly.input.day.description", "1 - 31", InputComponentDataType.TEXT))
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.monthly.input.time.name",
        "time", "scheduler.trigger.monthly.input.time.description", InputComponentDataType.DATE))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.createIfNotExists();
    contentDatabaseTable.createIndexIfNotExists("day");
    contentDatabaseTable.createIndexIfNotExists("time");
  }

  @Override
  public CompletableFuture<Void> insert(UUID triggerId, Map<String, Object> content) {
    return contentDatabaseTable.insertContent(triggerId,
      DatabaseRow.of(Integer.valueOf((String) content.get("day")),
        content.get("time")));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("day", String.valueOf(row.findCell(1).integerValue()),
        "time", row.findCell(2).stringValue()));
  }

  @Override
  public CompletableFuture<List<UUID>> findEntries(DatabaseCondition condition) {
    return contentDatabaseTable.findContentByCondition(condition).thenApply(
      rows -> rows.stream().map(row -> row.findCell(0).uuidValue()).toList());
  }

  @Override
  public CompletableFuture<Void> delete(UUID triggerId) {
    return contentDatabaseTable.deleteContent(triggerId);
  }
}