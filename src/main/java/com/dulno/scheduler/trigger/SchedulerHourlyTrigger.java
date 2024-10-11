package com.dulno.scheduler.trigger;

import com.dulno.core.database.*;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.trigger.Trigger;
import com.dulno.core.trigger.TriggerContentDatabaseTable;
import com.dulno.core.trigger.TriggerInformation;
import com.dulno.core.workflow.component.input.InputComponentDataType;
import com.dulno.core.workflow.component.input.InputComponentVariable;
import com.dulno.core.workflow.component.output.OutputComponentVariable;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public final class SchedulerHourlyTrigger implements Trigger {
  public static SchedulerHourlyTrigger create(
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("offset", DatabaseDataType.INT));
    return new SchedulerHourlyTrigger(
      TriggerContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "trigger_scheduler_hourly", contentColumns));
  }

  private final TriggerContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "scheduler-hourly-trigger";
  }

  @Override
  public TriggerInformation information() {
    return TriggerInformation.builder()
      .withName("scheduler.trigger.hourly.name")
      .withDescription("scheduler.trigger.hourly.description")
      .withInputVariable(InputComponentVariable.createRequired("scheduler.trigger.hourly.input.offset.name",
        "offset", "scheduler.trigger.hourly.input.offset.description", InputComponentDataType.NUMBER))
      .withOutputVariable(OutputComponentVariable.create("scheduler.trigger.hourly.input.offset.interval", "offset"))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.createIfNotExists();
    contentDatabaseTable.createIndexIfNotExists("offset");
  }

  @Override
  public CompletableFuture<Void> insert(UUID triggerId, Map<String, Object> content) {
    return contentDatabaseTable.insertContent(triggerId,
      DatabaseRow.of(Integer.valueOf((String) content.get("offset"))));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("offset", String.valueOf(row.findCell(1).integerValue())));
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