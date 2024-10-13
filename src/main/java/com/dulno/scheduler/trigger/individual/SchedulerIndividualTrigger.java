package com.dulno.scheduler.trigger.individual;

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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public final class SchedulerIndividualTrigger implements SchedulerTrigger {
  public static SchedulerIndividualTrigger create(
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("minute", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("hour", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("dayWeek", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("dayMonth", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("month", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("timezone", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("nextExecution", DatabaseDataType.BIGINT));
    return new SchedulerIndividualTrigger(
      TriggerContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "trigger_scheduler_individual", contentColumns));
  }

  private final TriggerContentDatabaseTable contentDatabaseTable;

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
    contentDatabaseTable.createIfNotExists();
    contentDatabaseTable.createIndexIfNotExists("nextExecution");
  }

  @Override
  public CompletableFuture<Void> insert(UUID triggerId, Map<String, Object> content) {
    content.put("minute", parseIndividualField((String) content.get("minute")));
    content.put("hour", parseIndividualField((String) content.get("hour")));
    content.put("dayWeek", parseIndividualField((String) content.get("dayWeek")));
    content.put("dayMonth", parseIndividualField((String) content.get("dayMonth")));
    content.put("month", parseIndividualField((String) content.get("month")));
    return contentDatabaseTable.insertContent(triggerId,
      DatabaseRow.of(content.get("minute"), content.get("hour"),
        content.get("dayWeek"), content.get("dayMonth"), content.get("month"),
        content.get("timezone"), calculateNextExecution(content)));
  }

  private static final Pattern INDIVIDUAL_FIELD_PATTERN = Pattern.compile(
    "^(\\*|([0-9]+(,[0-9]+)*|[0-9]+(-[0-9]+)?)(/[0-9]+)?)$");

  private String parseIndividualField(String individualField) {
    individualField = individualField.trim().replace(" ", "");
    if (!INDIVIDUAL_FIELD_PATTERN.matcher(individualField).matches()) {
      return "*";
    }
    return individualField;
  }

  @Override
  public CompletableFuture<Void> updateNextExecution(
    UUID triggerId, Map<String, Object> content
  ) {
    return contentDatabaseTable.updateContent(triggerId,
      DatabaseRow.of(content.get("minute"), content.get("hour"),
        content.get("dayWeek"), content.get("dayMonth"), content.get("month"),
        content.get("timezone"), calculateNextExecution(content)));
  }

  @Override
  public long calculateNextExecution(Map<String, Object> content) {
    var zoneId = ZoneId.of((String) content.get("timezone"));
    var minute = (String) content.get("minute");
    var hour = (String) content.get("hour");
    var dayWeek = (String) content.get("dayWeek");
    var dayMonth = (String) content.get("dayMonth");
    var month = (String) content.get("month");
    var now = ZonedDateTime.now(zoneId);
    var nextTime = now.withSecond(0).withNano(0).plusMinutes(1);
    while (!matchesIndividual(nextTime, minute, hour, dayWeek, dayMonth, month)) {
      nextTime = nextTime.plusMinutes(1);
    }
    return nextTime.toInstant().toEpochMilli() - 1000 * 30;
  }

  private boolean matchesIndividual(
    ZonedDateTime dateTime, String minute, String hour, String dayOfWeek,
    String dayOfMonth, String month
  ) {
    return matchesField(dateTime.getMinute(), minute) &&
      matchesField(dateTime.getHour(), hour) &&
      matchesField(dateTime.getDayOfWeek().getValue(), dayOfWeek) &&
      matchesField(dateTime.getDayOfMonth(), dayOfMonth) &&
      matchesField(dateTime.getMonthValue(), month);
  }

  private boolean matchesField(int value, String individualField) {
    if (isWildcard(individualField)) {
      return true;
    }
    var parts = individualField.split(",");
    for (var part : parts) {
      if (isStep(part)) {
        if (matchesStep(value, part)) {
          return true;
        }
      } else if (isRange(part)) {
        if (matchesRange(value, part)) {
          return true;
        } else {
          if (value == Integer.parseInt(part)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private boolean isWildcard(String individualField) {
    return individualField.equals("*");
  }

  private boolean isStep(String part) {
    return part.contains("/");
  }

  private boolean matchesStep(int value, String part) {
    var stepParts = part.split("/");
    var step = Integer.parseInt(stepParts[1]);
    if (stepParts[0].equals("*")) {
      return value % step == 0;
    } else {
      return matchesStepRange(value, stepParts[0], step);
    }
  }

  private boolean matchesStepRange(int value, String rangePart, int step) {
    var range = rangePart.split("-");
    var start = Integer.parseInt(range[0]);
    var end = Integer.parseInt(range[1]);
    for (var i = start; i <= end; i += step) {
      if (value == i) {
        return true;
      }
    }
    return false;
  }

  private boolean isRange(String part) {
    return part.contains("-");
  }

  private boolean matchesRange(int value, String part) {
    var range = part.split("-");
    var start = Integer.parseInt(range[0]);
    var end = Integer.parseInt(range[1]);
    return value >= start && value <= end;
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("minute", row.findCell(1).stringValue(),
        "hour", row.findCell(2).stringValue(),
        "dayWeek", row.findCell(3).stringValue(),
        "dayMonth", row.findCell(4).stringValue(),
        "month", row.findCell(5).stringValue(),
        "timezone", row.findCell(6).stringValue()));
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