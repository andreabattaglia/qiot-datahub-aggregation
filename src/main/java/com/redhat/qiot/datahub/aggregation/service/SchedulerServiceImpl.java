/**
 * 
 */
package com.redhat.qiot.datahub.aggregation.service;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;

import com.redhat.qiot.datahub.aggregation.util.events.AggregateHourToDayTimer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateMinuteToHourTimer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateNH3Timer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateOxidisingTimer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregatePM10Timer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregatePM2_5Timer;

import io.quarkus.scheduler.Scheduled;

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

    @Scheduled(every = "1m")
    void aggregateCoarseToMinuteInt() {
        aggregateCoarseToMinute(0L);
    }

    @Override
    public void aggregateCoarseToMinute(Long min) {
        LOGGER.info("aggregateCoarseToMinute() - start");

        aggregateNH3TimerEvent.fire(min);
        aggregateOxidisingTimerEvent.fire(min);
        aggregatePM10TimerEvent.fire(min);
        aggregatePM2_5TimerEvent.fire(min);

        LOGGER.info("aggregateCoarseToMinute() - end");
    }

    @Scheduled(every = "1h")
    void aggregateMinuteToHourInt() {
        aggregateMinuteToHour(1L);
    }

    @Override
    public void aggregateMinuteToHour(Long min) {
        LOGGER.info("aggregateMinuteToHour() - start");

        aggregateMinuteToHourTimerEvent.fire(min);

        LOGGER.info("aggregateMinuteToHour() - end");
    }

    @Scheduled(every = "24h")
    void aggregateHourToDayInt() {
        aggregateHourToDay(1L);
    }

    @Override
    public void aggregateHourToDay(Long min) {
        LOGGER.info("aggregateHourToDay() - start");

        aggregateHourToDayTimerEvent.fire(min);

        LOGGER.info("aggregateHourToDay() - end");
    }

}
