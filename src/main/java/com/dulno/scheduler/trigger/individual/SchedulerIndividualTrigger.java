package com.dulno.scheduler.trigger.individual;

import com.dulno.core.database.*;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.workflow.trigger.TriggerContentDatabaseTable;
import com.dulno.workflow.trigger.TriggerInformation;
import com.dulno.workflow.component.input.InputComponentDataType;
import com.dulno.workflow.component.input.InputComponentVariable;
import com.dulno.scheduler.trigger.CronTriggerContext;
import com.dulno.scheduler.trigger.SchedulerTrigger;
import com.dulno.scheduler.trigger.TriggerSchedulerDatabaseTable;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.support.CronTrigger;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public final class SchedulerIndividualTrigger implements SchedulerTrigger {
  public static SchedulerIndividualTrigger create(
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("cronMinute", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("hour", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("dayWeek", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("dayMonth", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("month", DatabaseDataType.TEXT));
    return new SchedulerIndividualTrigger(
      TriggerSchedulerDatabaseTable.create(databaseConnection, databaseKeyspace,
        "trigger_scheduler_individual", contentColumns));
  }

  private final TriggerSchedulerDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "scheduler-individual-trigger";
  }

  @Override
  public TriggerInformation information() {
    return TriggerInformation.builder()
      .withName("scheduler.trigger.individual.name")
      .withDescription("scheduler.trigger.individual.description")
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.individual.input.minute.name",
        "minute", "scheduler.trigger.individual.input.minute.description", InputComponentDataType.TEXT))
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.individual.input.hour.name",
        "hour", "scheduler.trigger.individual.input.hour.description", InputComponentDataType.TEXT))
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.individual.input.day.week.name",
        "dayWeek", "scheduler.trigger.individual.input.day.week.description", InputComponentDataType.TEXT))
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.individual.input.day.month.name",
        "dayMonth", "scheduler.trigger.individual.input.day.month.description", InputComponentDataType.TEXT))
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.individual.input.month.name",
        "month", "scheduler.trigger.individual.input.month.description", InputComponentDataType.TEXT))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.initialize();
  }

  @Override
  public CompletableFuture<Void> insert(UUID triggerId, Map<String, Object> content) {
    return contentDatabaseTable.insertContent(triggerId,
      findMinuteBucket(content), calculateNextExecution(content),
      (String) content.get("timezone"), DatabaseRow.of(content.get("minute"),
        content.get("hour"), content.get("dayWeek"), content.get("dayMonth"),
        content.get("month")));
  }

  @Override
  public CompletableFuture<Void> updateNextExecution(
    UUID triggerId, Map<String, Object> content
  ) {
    return contentDatabaseTable.updateContent(triggerId,
      findMinuteBucket(content), calculateNextExecution(content),
      (String) content.get("timezone"), DatabaseRow.of(content.get("minute"),
        content.get("hour"), content.get("dayWeek"), content.get("dayMonth"),
        content.get("month")));
  }

  private int findMinuteBucket(Map<String, Object> content) {
    try {
      return Integer.parseInt((String) content.get("minute"));
    } catch (Exception exception) {
      return -1;
    }
  }

  @Override
  public long calculateNextExecution(Map<String, Object> content) {
    var zoneId = ZoneId.of((String) content.get("timezone"));
    var minute = trimIndividualField((String) content.get("minute"));
    var hour = trimIndividualField((String) content.get("hour"));
    var dayWeek = trimIndividualField((String) content.get("dayWeek"));
    var dayMonth = trimIndividualField((String) content.get("dayMonth"));
    var month = trimIndividualField((String) content.get("month"));
    var cron = String.format("0 %s %s %s %s %s", minute, hour, dayMonth, month,
      dayWeek);
    try {
      var trigger = new CronTrigger(cron, zoneId);
      var nextExecution = trigger.nextExecution(CronTriggerContext.create());
      return nextExecution.toEpochMilli() - 1000 * 30;
    } catch (Exception exception) {
      return -1;
    }
  }

  private String trimIndividualField(String field) {
    return field.trim().replaceAll(" ", "");
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("minute", row.findCell(4).stringValue(),
        "hour", row.findCell(5).stringValue(),
        "dayWeek", row.findCell(6).stringValue(),
        "dayMonth", row.findCell(7).stringValue(),
        "month", row.findCell(8).stringValue(),
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