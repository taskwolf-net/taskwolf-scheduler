package net.taskwolf.scheduler;

import net.taskwolf.core.account.AccountLink;
import net.taskwolf.core.account.AccountLinkEntry;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "create")
public final class SchedulerAccountLink implements AccountLink {
  @Override
  public CompletableFuture<Boolean> accountExists(UUID userId) {
    return CompletableFuture.completedFuture(true);
  }

  @Override
  public CompletableFuture<List<AccountLinkEntry>> findAccounts(UUID userId) {
    return CompletableFuture.completedFuture(Lists.newArrayList());
  }

  @Override
  public void removeAccount(UUID userId, String identifier) {

  }

  @Override
  public String registrationUrl(UUID id, String apiKey) {
    return "";
  }

  @Override
  public String description() {
    return "scheduler.link.description";
  }
}
