package com.dulno.scheduler;

import com.dulno.core.CoreModule;
import com.dulno.core.account.AccountLink;
import com.dulno.core.action.ActionRepository;
import com.dulno.core.database.DatabaseConnection;
import com.dulno.core.database.DatabaseKeyspace;
import com.dulno.core.log.Log;
import com.dulno.core.module.Module;
import com.dulno.core.module.ModuleDescription;
import com.dulno.core.module.ModuleInformation;
import com.dulno.core.module.ModuleLoadPriority;
import com.dulno.core.trigger.TriggerRepository;
import com.dulno.scheduler.trigger.SchedulerTriggerSchedule;
import com.dulno.scheduler.trigger.daily.SchedulerDailyTrigger;
import com.dulno.scheduler.trigger.hourly.SchedulerHourlyTrigger;
import com.dulno.scheduler.trigger.individual.SchedulerIndividualTrigger;
import com.dulno.scheduler.trigger.monthly.SchedulerMonthlyTrigger;
import com.dulno.scheduler.trigger.weekly.SchedulerWeeklyTrigger;
import com.dulno.scheduler.trigger.weekly.WeekDayComponentSelect;
import com.dulno.scheduler.trigger.yearly.SchedulerYearlyTrigger;
import com.google.inject.Injector;

@ModuleDescription(name = "scheduler", version = "1.0.0-SNAPSHOT",
  priority = ModuleLoadPriority.NEUTRAL)
public final class SchedulerModule extends Module {
  private Log log;
  private AccountLink accountLink;
  private SchedulerTriggerSchedule schedulerTriggerSchedule;

  public SchedulerModule(Injector injector) {
    super(injector);
  }

  @Override
  public void enable() throws Exception {
    log = injector().getInstance(Log.class).subLog("Scheduler");
    accountLink = SchedulerAccountLink.create();
    schedulerTriggerSchedule = SchedulerTriggerSchedule.create(
      injector().getInstance(CoreModule.class), triggerRepository());
    schedulerTriggerSchedule.start();

  }

  @Override
  public void disable() {
    schedulerTriggerSchedule.stop();
  }

  @Override
  public AccountLink accountLink() {
    return accountLink;
  }

  @Override
  public ModuleInformation moduleInformation() {
    return ModuleInformation.create("scheduler", "", "scheduler.png",
      ModuleInformation.Type.PUBLIC);
  }

  @Override
  public TriggerRepository triggerRepository() {
    var databaseConnection = injector().getInstance(DatabaseConnection.class);
    var databaseKeyspace = injector().getInstance(DatabaseKeyspace.class);
    var repository = TriggerRepository.create();
    repository.registerTrigger(SchedulerHourlyTrigger.create(databaseConnection,
      databaseKeyspace));
    repository.registerTrigger(SchedulerDailyTrigger.create(databaseConnection,
      databaseKeyspace));
    repository.registerTrigger(SchedulerWeeklyTrigger.create(databaseConnection,
      databaseKeyspace, injector().getInstance(WeekDayComponentSelect.class)));
    repository.registerTrigger(SchedulerMonthlyTrigger.create(databaseConnection,
      databaseKeyspace));
    repository.registerTrigger(SchedulerYearlyTrigger.create(databaseConnection,
      databaseKeyspace));
    repository.registerTrigger(SchedulerIndividualTrigger.create(databaseConnection,
      databaseKeyspace));
    return repository;
  }

  @Override
  public ActionRepository actionRepository() {
    return ActionRepository.create();
  }
}