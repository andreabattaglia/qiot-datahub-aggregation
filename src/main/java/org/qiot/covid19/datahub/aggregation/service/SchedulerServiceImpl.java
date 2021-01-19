/**
 * 
 */
package org.qiot.covid19.datahub.aggregation.service;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.qiot.covid19.datahub.aggregation.util.events.AggregateAll;
import org.qiot.covid19.datahub.aggregation.util.events.AggregateHourToDayTimer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregateMinuteToHourTimer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregateNH3Timer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregateOxidisingTimer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregatePM10Timer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregatePM2_5Timer;
import org.slf4j.Logger;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import io.quarkus.scheduler.Scheduled.Schedules;

/**
 * @author abattagl
 *
 */
@ApplicationScoped
class SchedulerServiceImpl implements SchedulerService {
    /**
     * Logger for this class
     */
    @Inject
    Logger LOGGER;

    @Inject
    @AggregateNH3Timer
    Event<Long> aggregateNH3TimerEvent;
    @Inject
    @AggregateOxidisingTimer
    Event<Long> aggregateOxidisingTimerEvent;

    @Inject
    @AggregatePM10Timer
    Event<Long> aggregatePM10TimerEvent;
    @Inject
    @AggregatePM2_5Timer
    Event<Long> aggregatePM2_5TimerEvent;
    
    @Inject
    @AggregateMinuteToHourTimer
    Event<Long> aggregateMinuteToHourTimerEvent;
    @Inject
    @AggregateHourToDayTimer
    Event<Long> aggregateHourToDayTimerEvent;

    @Inject
    @AggregateAll
    Event<Long> aggregateAllEvent;

//    @Scheduled(every = "1m")
    @Scheduled(cron = "{aggr.minute.cron.expr}",concurrentExecution = ConcurrentExecution.SKIP)
    void _aggregateCoarseToMinute() {
        LOGGER.info("aggregateCoarseToMinute() - start");

        long minTime = 2L;
        aggregateNH3TimerEvent.fire(minTime);
        aggregateOxidisingTimerEvent.fire(minTime);
        aggregatePM10TimerEvent.fire(minTime);
        aggregatePM2_5TimerEvent.fire(minTime);

        LOGGER.info("aggregateCoarseToMinute() - end");
    }

//    @Scheduled(every = "1h")
    @Scheduled(cron = "{aggr.hour.cron.expr}",concurrentExecution = ConcurrentExecution.SKIP)
    void aggregateMinuteToHourInt() {
        LOGGER.info("aggregateMinuteToHour() - start");

        aggregateMinuteToHourTimerEvent.fire(1L);

        LOGGER.info("aggregateMinuteToHour() - end");
    }


//    @Scheduled(every = "24h")
    @Scheduled(cron = "{aggr.day.cron.expr}",concurrentExecution = ConcurrentExecution.SKIP)
    void aggregateHourToDayInt() {
        LOGGER.info("aggregateHourToDay() - start");

        aggregateHourToDayTimerEvent.fire(1L);

        LOGGER.info("aggregateHourToDay() - end");
    }

}
