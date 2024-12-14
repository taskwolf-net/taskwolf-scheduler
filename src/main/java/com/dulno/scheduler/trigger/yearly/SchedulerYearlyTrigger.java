package com.dulno.scheduler.trigger.yearly;

import com.dulno.core.database.*;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.trigger.TriggerInformation;
import com.dulno.core.workflow.component.input.InputComponentDataType;
import com.dulno.core.workflow.component.input.InputComponentVariable;
import com.dulno.scheduler.trigger.CronTriggerContext;
import com.dulno.scheduler.trigger.SchedulerTrigger;
import com.dulno.scheduler.trigger.TriggerSchedulerDatabaseTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.support.CronTrigger;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public final class SchedulerYearlyTrigger implements SchedulerTrigger {
  public static SchedulerYearlyTrigger create(
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("day", DatabaseDataType.INT));
    contentColumns.add(DatabaseColumn.create("month", DatabaseDataType.INT));
    contentColumns.add(DatabaseColumn.create("time", DatabaseDataType.TEXT));
    return new SchedulerYearlyTrigger(
      TriggerSchedulerDatabaseTable.create(databaseConnection, databaseKeyspace,
        "trigger_scheduler_yearly", contentColumns));
  }

  private final TriggerSchedulerDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "scheduler-yearly-trigger";
  }

  @Override
  public TriggerInformation information() {
    return TriggerInformation.builder()
      .withName("scheduler.trigger.yearly.name")
      .withDescription("scheduler.trigger.yearly.description")
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.yearly.input.day.name",
        "day", "scheduler.trigger.yearly.input.day.description", "1 - 31", InputComponentDataType.TEXT))
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.yearly.input.month.name",
        "month", "scheduler.trigger.yearly.input.month.description", "1 - 12", InputComponentDataType.TEXT))
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.yearly.input.time.name",
        "time", "scheduler.trigger.yearly.input.time.description", InputComponentDataType.TIME))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.initialize();
  }

  @Override
  public CompletableFuture<Void> insert(UUID triggerId, Map<String, Object> content) {
    content.put("day", parseDay(content));
    content.put("month", parseMonth(content));
    var time = parseTime(content);
    content.put("time", time);
    return contentDatabaseTable.insertContent(triggerId,
      Integer.valueOf(time.split(":")[1]), calculateNextExecution(content),
      (String) content.get("timezone"), DatabaseRow.of(content.get("day"),
        content.get("month"), time));
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

  private int parseMonth(Map<String, Object> content) {
    try {
      var month = content.get("month");
      if (month == null) {
        return 1;
      }
      var value = Integer.parseInt((String) month);
      if (value < 1 || value > 12) {
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
    content.put("month", Integer.parseInt((String) content.get("month")));
    var time = (String) content.get("time");
    return contentDatabaseTable.updateContent(triggerId,
      Integer.valueOf(time.split(":")[1]), calculateNextExecution(content),
      (String) content.get("timezone"), DatabaseRow.of(content.get("day"),
        content.get("month"), time));
  }

  @Override
  public long calculateNextExecution(Map<String, Object> content) {
    var zoneId = ZoneId.of((String) content.get("timezone"));
    var parts = ((String) content.get("time")).split(":");
    var hour = Integer.parseInt(parts[0]);
    var minute = Integer.parseInt(parts[1]);
    var day = (int) content.get("day");
    var month = (int) content.get("month");
    var cron = String.format("0 %s %s %s %s *", minute, hour, day, month);
    try {
      var trigger = new CronTrigger(cron, zoneId);
      var nextExecution = trigger.nextExecution(CronTriggerContext.create());
      return nextExecution.toEpochMilli() - 1000 * 30;
    } catch (Exception exception) {
      return -1;
    }
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("day", String.valueOf(row.findCell(4).integerValue()),
        "month", String.valueOf(row.findCell(5).integerValue()),
        "time", row.findCell(6).stringValue(),
        "timezone", row.findCell(3).stringValue()));
  }

  @Override
  public CompletableFuture<List<UUID>> findEntries(DatabaseCondition condition) {
    return contentDatabaseTable.findContentByCondition(condition).thenApply(
      rows -> rows.stream().map(row -> row.findCell(2).uuidValue()).toList());
  }

  @Override
  public CompletableFuture<Void> delete(UUID triggerId) {
    return contentDatabaseTable.deleteContent(triggerId);
  }
}