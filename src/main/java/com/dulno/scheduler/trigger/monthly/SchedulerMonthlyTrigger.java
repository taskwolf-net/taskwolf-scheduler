package com.dulno.scheduler.trigger.monthly;

import com.dulno.core.database.*;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.trigger.TriggerContentDatabaseTable;
import com.dulno.core.trigger.TriggerInformation;
import com.dulno.core.workflow.component.input.InputComponentDataType;
import com.dulno.core.workflow.component.input.InputComponentVariable;
import com.dulno.scheduler.trigger.SchedulerTrigger;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public final class SchedulerMonthlyTrigger implements SchedulerTrigger {
  public static SchedulerMonthlyTrigger create(
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("day", DatabaseDataType.INT));
    contentColumns.add(DatabaseColumn.create("time", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("timezone", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("nextExecution", DatabaseDataType.BIGINT));
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
        "time", "scheduler.trigger.monthly.input.time.description", InputComponentDataType.TIME))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.createIfNotExists();
    contentDatabaseTable.createIndexIfNotExists("nextExecution");
  }

  @Override
  public CompletableFuture<Void> insert(UUID triggerId, Map<String, Object> content) {
    content.put("day", parseDay(content));
    content.put("time", parseTime(content));
    return contentDatabaseTable.insertContent(triggerId,
      DatabaseRow.of(content.get("day"), content.get("time"),
        content.get("timezone"), calculateNextExecution(content)));
  }

  private int parseDay(Map<String, Object> content) {
    try {
      var day = content.get("day");
      if (day == null) {
        return 1;
      }
      var value = Integer.parseInt((String) day);
      if (value < 1 || value > 31) {
        return 1;
      }
      return value;
    } catch (Exception exception) {
      return 1;
    }
  }

  private String parseTime(Map<String, Object> content) {
    var time = content.get("time");
    if (time == null) {
      return "00:00";
    }
    var value = (String) time;
    if (!value.matches("^([01]\\d|2[0-3]):([0-5]\\d)$")) {
      return "00:00";
    }
    return value;
  }

  @Override
  public CompletableFuture<Void> updateNextExecution(
    UUID triggerId, Map<String, Object> content
  ) {
    content = Maps.newHashMap(content);
    content.put("day", Integer.parseInt((String) content.get("day")));
    return contentDatabaseTable.updateContent(triggerId,
      DatabaseRow.of(content.get("day"), content.get("time"),
        content.get("timezone"), calculateNextExecution(content)));
  }

  @Override
  public long calculateNextExecution(Map<String, Object> content) {
    var zoneId = ZoneId.of((String) content.get("timezone"));
    var now = LocalDateTime.now(zoneId);
    var parts = ((String) content.get("time")).split(":");
    var dayOfMonth = (int) content.get("day");
    var targetTime = now.withDayOfMonth(Math.min(dayOfMonth,
        now.getMonth().length(now.toLocalDate().isLeapYear())))
      .withHour(Integer.parseInt(parts[0]))
      .withMinute(Integer.parseInt(parts[1]))
      .withSecond(0).withNano(0);
    if (targetTime.isBefore(now)) {
      targetTime = targetTime.plusMonths(1)
        .withDayOfMonth(Math.min(dayOfMonth, targetTime.getMonth()
          .length(now.toLocalDate().isLeapYear())));
    }
    return targetTime.atZone(zoneId).toInstant().toEpochMilli() - 1000 * 30;
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("day", String.valueOf(row.findCell(1).integerValue()),
        "time", row.findCell(2).stringValue(),
        "timezone", row.findCell(3).stringValue()));
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