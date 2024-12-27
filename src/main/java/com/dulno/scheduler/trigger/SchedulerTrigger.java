package com.dulno.scheduler.trigger;

import com.dulno.workflow.trigger.Trigger;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface SchedulerTrigger extends Trigger {
  long calculateNextExecution(Map<String, Object> content);

  CompletableFuture<Void> updateNextExecution(UUID triggerId,
    Map<String, Object> content);
}
