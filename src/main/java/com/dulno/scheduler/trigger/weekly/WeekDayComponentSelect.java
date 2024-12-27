package com.dulno.scheduler.trigger.weekly;

import com.dulno.core.locale.Translation;
import com.dulno.core.user.User;
import com.dulno.workflow.component.input.InputComponentSelect;
import com.dulno.workflow.component.input.InputComponentSelectEntry;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Singleton
@RequiredArgsConstructor(access = AccessLevel.PRIVATE, onConstructor = @__({@Inject}))
public final class WeekDayComponentSelect implements InputComponentSelect {
  private final Translation translation;

  @Override
  public CompletableFuture<List<InputComponentSelectEntry>> compile(
    User user, UUID id, Map<String, String> previousInputs
  ) {
    var days = Lists.<InputComponentSelectEntry>newArrayList();
    days.add(InputComponentSelectEntry.create("1",
      translation.translate(user, "scheduler.trigger.weekly.monday")));
    days.add(InputComponentSelectEntry.create("2",
      translation.translate(user, "scheduler.trigger.weekly.tuesday")));
    days.add(InputComponentSelectEntry.create("3",
      translation.translate(user, "scheduler.trigger.weekly.wednesday")));
    days.add(InputComponentSelectEntry.create("4",
      translation.translate(user, "scheduler.trigger.weekly.thursday")));
    days.add(InputComponentSelectEntry.create("5",
      translation.translate(user, "scheduler.trigger.weekly.friday")));
    days.add(InputComponentSelectEntry.create("6",
      translation.translate(user, "scheduler.trigger.weekly.saturday")));
    days.add(InputComponentSelectEntry.create("7",
      translation.translate(user, "scheduler.trigger.weekly.sunday")));
    return CompletableFuture.completedFuture(days);
  }
}