package net.taskwolf.scheduler;

import net.taskwolf.core.account.AccountLink;
import net.taskwolf.workflow.WorkflowModule;
import net.taskwolf.workflow.action.ActionRepository;
import net.taskwolf.core.database.DatabaseConnection;
import net.taskwolf.core.database.DatabaseKeyspace;
import net.taskwolf.core.log.Log;
import net.taskwolf.core.module.ModuleDescription;
import net.taskwolf.core.module.ModuleInformation;
import net.taskwolf.core.module.ModuleLoadPriority;
import net.taskwolf.workflow.integration.Integration;
import net.taskwolf.workflow.trigger.TriggerRepository;
import net.taskwolf.scheduler.trigger.SchedulerTriggerSchedule;
import net.taskwolf.scheduler.trigger.daily.SchedulerDailyTrigger;
import net.taskwolf.scheduler.trigger.hourly.SchedulerHourlyTrigger;
import net.taskwolf.scheduler.trigger.individual.SchedulerIndividualTrigger;
import net.taskwolf.scheduler.trigger.monthly.SchedulerMonthlyTrigger;
import net.taskwolf.scheduler.trigger.weekly.SchedulerWeeklyTrigger;
import net.taskwolf.scheduler.trigger.weekly.WeekDayComponentSelect;
import net.taskwolf.scheduler.trigger.yearly.SchedulerYearlyTrigger;
import com.google.inject.Injector;

@ModuleDescription(name = "scheduler", version = "1.0.0-SNAPSHOT",
  priority = ModuleLoadPriority.NEUTRAL)
public final class SchedulerModule extends Integration {
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
      injector().getInstance(WorkflowModule.class), triggerRepository());
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