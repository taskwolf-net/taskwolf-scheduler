package com.dulno.scheduler.trigger;

import com.dulno.core.CoreModule;
import com.dulno.core.database.condition.DatabaseCondition;
import com.google.common.collect.Maps;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor(staticName = "create")
public final class SchedulerTriggerSchedule {
  private final CoreModule coreModule;
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
    executeHourlyTriggers();;
    executeDailyTriggers();
    executeWeeklyTriggers();
    executeMonthlyTriggers();
    executeYearlyTriggers();
    executeIndividualTriggers();
  }

  private void executeHourlyTriggers() {
    var currentMinute = LocalTime.now().getMinute();
    coreModule.triggerWorkflows("scheduler", "scheduler-hourly-trigger",
      DatabaseCondition.of("offset", currentMinute), Maps.newHashMap());
  }

  private void executeDailyTriggers() {
    var currentTime = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm"));
    coreModule.triggerWorkflows("scheduler", "scheduler-daily-trigger",
      DatabaseCondition.of("time", currentTime), Maps.newHashMap());
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

  public void stop() {
    scheduler.cancel(false);
  }
}
