package net.taskwolf.scheduler.trigger.weekly;

import net.taskwolf.core.database.*;
import net.taskwolf.core.database.condition.DatabaseCondition;
import net.taskwolf.workflow.trigger.TriggerInformation;
import net.taskwolf.workflow.component.input.InputComponentDataType;
import net.taskwolf.workflow.component.input.InputComponentVariable;
import net.taskwolf.scheduler.trigger.SchedulerTrigger;
import net.taskwolf.scheduler.trigger.TriggerSchedulerDatabaseTable;
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
public final class SchedulerWeeklyTrigger implements SchedulerTrigger {
  public static SchedulerWeeklyTrigger create(
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace,
    WeekDayComponentSelect weekDayComponentSelect
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("day", DatabaseDataType.INT));
    contentColumns.add(DatabaseColumn.create("time", DatabaseDataType.TEXT));
    return new SchedulerWeeklyTrigger(
      TriggerSchedulerDatabaseTable.create(databaseConnection, databaseKeyspace,
        "trigger_scheduler_weekly", contentColumns), weekDayComponentSelect);
  }

  private final TriggerSchedulerDatabaseTable contentDatabaseTable;
  private final WeekDayComponentSelect weekDayComponentSelect;

  @Override
  public String type() {
    return "scheduler-weekly-trigger";
  }

  @Override
  public TriggerInformation information() {
    return TriggerInformation.builder()
      .withName("scheduler.trigger.weekly.name")
      .withDescription("scheduler.trigger.weekly.description")
      .withInputVariable(InputComponentVariable.createSelect("scheduler.trigger.weekly.input.day.name",
        "day", "scheduler.trigger.weekly.input.day.description", weekDayComponentSelect))
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.weekly.input.time.name",
        "time", "scheduler.trigger.weekly.input.time.description", InputComponentDataType.TIME))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.initialize();
  }

  @Override
  public CompletableFuture<Void> insert(
    UUID triggerId, UUID ownerId, Map<String, Object> content
  ) {
    content.put("day", parseDay(content));
    var time = parseTime(content);
    content.put("time", time);
    return contentDatabaseTable.insertContent(triggerId,
      Integer.valueOf(time.split(":")[1]), calculateNextExecution(content),
      (String) content.get("timezone"), DatabaseRow.of(content.get("day"), time));
  }

  private int parseDay(Map<String, Object> content) {
    try {
      var day = content.get("day");
      if (day == null) {
        return 1;
      }
      var value = Integer.parseInt((String) day);
      if (value < 1 || value > 7) {
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
    var time = (String) content.get("time");
    return contentDatabaseTable.updateContent(triggerId,
      Integer.valueOf(time.split(":")[1]), calculateNextExecution(content),
      (String) content.get("timezone"), DatabaseRow.of(content.get("day"), time));
  }

  @Override
  public long calculateNextExecution(Map<String, Object> content) {
    var zoneId = ZoneId.of((String) content.get("timezone"));
    var now = LocalDateTime.now(zoneId);
    var parts = ((String) content.get("time")).split(":");
    var targetTime = now.withHour(Integer.parseInt(parts[0]))
      .withMinute(Integer.parseInt(parts[1]))
      .withSecond(0).withNano(0);
    int daysUntilTarget = (((int) content.get("day")) -
      now.getDayOfWeek().getValue() + 7) % 7;
    if (daysUntilTarget == 0 && targetTime.isBefore(now)) {
      daysUntilTarget = 7;
    }
    targetTime = targetTime.plusDays(daysUntilTarget);
    return targetTime.atZone(zoneId).toInstant().toEpochMilli() - 1000 * 30;
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("day", String.valueOf(row.findCell(4).integerValue()),
        "time", row.findCell(5).stringValue(),
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