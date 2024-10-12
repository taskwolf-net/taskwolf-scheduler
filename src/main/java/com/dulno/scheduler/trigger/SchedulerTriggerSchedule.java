package com.dulno.scheduler.trigger;

import com.dulno.core.CoreModule;
import com.dulno.core.database.condition.DatabaseComparison;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.trigger.TriggerRepository;
import com.google.common.collect.Maps;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor(staticName = "create")
public final class SchedulerTriggerSchedule {
  private final CoreModule coreModule;
  private final TriggerRepository triggerRepository;
  private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
  private ScheduledFuture<?> scheduler;

  private static final int SCHEDULER_INTERVAL = 1000 * 60;
  private static final TimeUnit SCHEDULER_TIME_UNIT = TimeUnit.MILLISECONDS;

  public void start() {
    scheduler = executorService.scheduleAtFixedRate(this::execute,
      calculateInitialDelay(), SCHEDULER_INTERVAL, SCHEDULER_TIME_UNIT);
  }

  private long calculateInitialDelay() {
    var start = LocalDateTime.now();
    var end = start.plusMinutes(1).truncatedTo(ChronoUnit.MINUTES);
    return Duration.between(start, end).toMillis();
  }

  private void execute() {
    executeSchedulerTriggers("scheduler-hourly-trigger");
    executeSchedulerTriggers("scheduler-daily-trigger");
    executeWeeklyTriggers();
    executeMonthlyTriggers();
    executeYearlyTriggers();
    executeIndividualTriggers();
  }

  private void executeWeeklyTriggers() {
    var currentDay = LocalDate.now().getDayOfWeek().getValue();
    var currentTime = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm"));
    coreModule.triggerWorkflows("scheduler", "scheduler-weekly-trigger",
      DatabaseCondition.of("day", currentDay, "time", currentTime,
        DatabaseCondition.Filtering.ALLOWED), Maps.newHashMap());
  }

  private void executeMonthlyTriggers() {
    var currentDay = LocalDate.now().getDayOfMonth();
    var currentTime = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm"));
    coreModule.triggerWorkflows("scheduler", "scheduler-monthly-trigger",
      DatabaseCondition.of("day", currentDay, "time", currentTime,
        DatabaseCondition.Filtering.ALLOWED), Maps.newHashMap());
  }

  private void executeYearlyTriggers() {
    var currentDay = LocalDate.now().getDayOfMonth();
    var currentMonth = LocalDate.now().getMonth().getValue();
    var currentTime = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm"));
    coreModule.triggerWorkflows("scheduler", "scheduler-yearly-trigger",
      DatabaseCondition.of("day", currentDay, "month", currentMonth,
        "time", currentTime, DatabaseCondition.Filtering.ALLOWED),
      Maps.newHashMap());
  }

  private void executeIndividualTriggers() {
    //TODO: TO BE IMPLEMENTED
  }

  private void executeSchedulerTriggers(String type) {
    var trigger = (SchedulerTrigger) triggerRepository.findTrigger(type).get();
    var condition = DatabaseCondition.of(DatabaseCondition.Filtering.ALLOWED,
      DatabaseComparison.create("nextExecution", System.currentTimeMillis(),
        DatabaseComparison.Type.SMALLER_EQUALS));
    coreModule.findSomeTriggerEntries("scheduler", type, condition)
      .thenAccept(entries -> entries.forEach(entry -> trigger.findContent(entry.id())
        .thenAccept(content -> trigger.updateNextExecution(entry.id(), content)
          .thenAccept(value -> executeSchedulerTrigger(entry.id())))));
  }

  private void executeSchedulerTrigger(UUID triggerId) {
    coreModule.createWorkflow(triggerId)
      .thenAccept(workflow -> workflow.trigger(Maps.newHashMap()));
  }

  public void stop() {
    scheduler.cancel(false);
  }
}
