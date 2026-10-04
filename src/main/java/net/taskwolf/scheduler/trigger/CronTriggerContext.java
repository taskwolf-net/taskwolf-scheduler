package net.taskwolf.scheduler.trigger;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.TriggerContext;

import java.time.Instant;

@RequiredArgsConstructor(staticName = "create")
public class CronTriggerContext implements TriggerContext {
  @Override
  public Instant lastScheduledExecution() {
    return null;
  }

  @Override
  public Instant lastActualExecution() {
    return null;
  }

  @Override
  public Instant lastCompletion() {
    return null;
  }
}
