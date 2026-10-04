package net.taskwolf.scheduler.trigger;

import net.taskwolf.core.database.condition.DatabaseComparison;
import net.taskwolf.core.database.condition.DatabaseCondition;
import net.taskwolf.workflow.WorkflowModule;
import net.taskwolf.workflow.trigger.TriggerRepository;
import com.google.common.collect.Maps;
import lombok.RequiredArgsConstructor;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor(staticName = "create")
public final class SchedulerTriggerSchedule {
  private final WorkflowModule workflowModule;
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
    var currentMinute = LocalTime.now().getSecond() >= 30 ?
      LocalTime.now().plusMinutes(1).getMinute() :
      LocalTime.now().getMinute();
    executeSchedulerTriggers("scheduler-hourly-trigger", currentMinute);
    executeSchedulerTriggers("scheduler-daily-trigger", currentMinute);
    executeSchedulerTriggers("scheduler-weekly-trigger", currentMinute);
    executeSchedulerTriggers("scheduler-monthly-trigger", currentMinute);
    executeSchedulerTriggers("scheduler-yearly-trigger", currentMinute);
    executeSchedulerTriggers("scheduler-individual-trigger", currentMinute);
    executeSchedulerTriggers("scheduler-individual-trigger", -1);
  }

  private void executeSchedulerTriggers(String type, int currentMinute) {
    var trigger = (SchedulerTrigger) triggerRepository.findTrigger(type).get();
    var condition = DatabaseCondition.of(
      DatabaseComparison.create("minute", currentMinute),
      DatabaseComparison.create("nextExecution", System.currentTimeMillis(),
        DatabaseComparison.Type.SMALLER_EQUALS),
      DatabaseComparison.create("nextExecution", 0L,
        DatabaseComparison.Type.GREATER_EQUALS));
    workflowModule.findSomeTriggerEntries("scheduler", type, condition)
      .thenAccept(entries -> entries.forEach(entry -> trigger.findContent(entry.id())
        .thenAccept(content -> trigger.updateNextExecution(entry.id(), content)
          .thenAccept(value -> executeSchedulerTrigger(entry.id())))));
  }

  private void executeSchedulerTrigger(UUID triggerId) {
    workflowModule.createWorkflow(triggerId)
      .thenAccept(workflow -> workflow.trigger(Maps.newHashMap()));
  }

  public void stop() {
    scheduler.cancel(false);
  }
}
