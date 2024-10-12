package com.dulno.scheduler.trigger.daily;

import com.dulno.core.database.*;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.trigger.TriggerContentDatabaseTable;
import com.dulno.core.trigger.TriggerInformation;
import com.dulno.core.workflow.component.input.InputComponentDataType;
import com.dulno.core.workflow.component.input.InputComponentVariable;
import com.dulno.scheduler.trigger.SchedulerTrigger;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public final class SchedulerDailyTrigger implements SchedulerTrigger {
  public static SchedulerDailyTrigger create(
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("time", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("timezone", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("nextExecution", DatabaseDataType.BIGINT));
    return new SchedulerDailyTrigger(
      TriggerContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "trigger_scheduler_daily", contentColumns));
  }

  private final TriggerContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "scheduler-daily-trigger";
  }

  @Override
  public TriggerInformation information() {
    return TriggerInformation.builder()
      .withName("scheduler.trigger.daily.name")
      .withDescription("scheduler.trigger.daily.description")
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.daily.input.time.name",
        "time", "scheduler.trigger.daily.input.time.description", InputComponentDataType.TIME))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.createIfNotExists();
    contentDatabaseTable.createIndexIfNotExists("nextExecution");
  }

  @Override
  public CompletableFuture<Void> insert(UUID triggerId, Map<String, Object> content) {
    return contentDatabaseTable.insertContent(triggerId,
      DatabaseRow.of(content.get("time"), content.get("timezone"),
        calculateNextExecution(content)));
  }

  @Override
  public CompletableFuture<Void> updateNextExecution(
    UUID triggerId, Map<String, Object> content
  ) {
    return contentDatabaseTable.updateContent(triggerId,
      DatabaseRow.of(content.get("time"), content.get("timezone"),
        calculateNextExecution(content)));
  }

  @Override
  public long calculateNextExecution(Map<String, Object> content) {
    var parts = ((String) content.get("time")).split(":");
    var targetHour = Integer.parseInt(parts[0]);
    var targetMinute = Integer.parseInt(parts[1]);
    var zoneId = ZoneId.of((String) content.get("timezone"));
    var now = LocalDateTime.now(zoneId);
    var nextTargetTime = now.withHour(targetHour).withMinute(targetMinute)
      .withSecond(0).withNano(0);
    if (nextTargetTime.isBefore(now)) {
      nextTargetTime = nextTargetTime.plusDays(1);
    }
    return nextTargetTime.atZone(zoneId).toInstant().toEpochMilli() - 1000 * 30;
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("time", row.findCell(1).stringValue(),
        "timezone", row.findCell(2).stringValue()));
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