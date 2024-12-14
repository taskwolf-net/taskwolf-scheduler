package com.dulno.scheduler.trigger.hourly;

import com.dulno.core.database.DatabaseConnection;
import com.dulno.core.database.DatabaseKeyspace;
import com.dulno.core.database.DatabaseRow;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.trigger.TriggerInformation;
import com.dulno.core.workflow.component.input.InputComponentDataType;
import com.dulno.core.workflow.component.input.InputComponentVariable;
import com.dulno.scheduler.trigger.SchedulerTrigger;
import com.dulno.scheduler.trigger.TriggerSchedulerDatabaseTable;
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
public final class SchedulerHourlyTrigger implements SchedulerTrigger {
  public static SchedulerHourlyTrigger create(
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    return new SchedulerHourlyTrigger(
      TriggerSchedulerDatabaseTable.create(databaseConnection, databaseKeyspace,
        "trigger_scheduler_hourly", Lists.newArrayList()));
  }

  private final TriggerSchedulerDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "scheduler-hourly-trigger";
  }

  @Override
  public TriggerInformation information() {
    return TriggerInformation.builder()
      .withName("scheduler.trigger.hourly.name")
      .withDescription("scheduler.trigger.hourly.description")
      .withInputVariable(InputComponentVariable.createOptional("scheduler.trigger.hourly.input.offset.name",
        "offset", "scheduler.trigger.hourly.input.offset.description", "0 - 59", InputComponentDataType.TEXT))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.initialize();
  }

  @Override
  public CompletableFuture<Void> insert(UUID triggerId, Map<String, Object> content) {
    content.put("offset", parseOffset(content));
    return contentDatabaseTable.insertContent(triggerId,
      (int) content.get("offset"), calculateNextExecution(content),
      (String) content.get("timezone"), DatabaseRow.of());
  }

  private int parseOffset(Map<String, Object> content) {
    try {
      var offset = content.get("offset");
      if (offset == null) {
        return 0;
      }
      var value = Integer.parseInt((String) offset);
      if (value < 0 || value > 59) {
        return 0;
      }
      return value;
    } catch (Exception exception) {
      return 0;
    }
  }

  @Override
  public CompletableFuture<Void> updateNextExecution(
    UUID triggerId, Map<String, Object> content
  ) {
    content = Maps.newHashMap(content);
    content.put("offset", Integer.parseInt((String) content.get("offset")));
    return contentDatabaseTable.updateContent(triggerId,
      (int) content.get("offset"), calculateNextExecution(content),
      (String) content.get("timezone"), DatabaseRow.of());
  }

  @Override
  public long calculateNextExecution(Map<String, Object> content) {
    var offset = (int) content.get("offset");
    var zoneId = ZoneId.of((String) content.get("timezone"));
    var now = LocalDateTime.now(zoneId);
    var nextTargetTime = now.withMinute(offset).withSecond(0).withNano(0);
    if (now.getMinute() >= offset) {
      nextTargetTime = nextTargetTime.plusHours(1);
    }
    return nextTargetTime.atZone(zoneId).toInstant().toEpochMilli() - 1000 * 30;
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("offset", String.valueOf(row.findCell(0).integerValue()),
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