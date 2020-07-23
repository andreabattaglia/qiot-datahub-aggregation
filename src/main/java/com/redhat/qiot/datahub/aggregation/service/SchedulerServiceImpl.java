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
    Event<String> aggregateNH3TimerEvent;

    @Inject
    @AggregateOxidisingTimer
    Event<String> aggregateOxidisingTimerEvent;

    @Inject
    @AggregatePM10Timer
    Event<String> aggregatePM10TimerEvent;

    @Inject
    @AggregatePM2_5Timer
    Event<String> aggregatePM2_5TimerEvent;
    @Inject
    @AggregateMinuteToHourTimer
    Event<String> aggregateMinuteToHourTimerEvent;
    @Inject
    @AggregateHourToDayTimer
    Event<String> aggregateHourToDayTimerEvent;

    @Scheduled(every = "1m")
    void aggregateCoarseToMinute() {
        LOGGER.info("aggregateCoarseToMinute() - start");

        aggregateNH3TimerEvent.fire("");
        aggregateOxidisingTimerEvent.fire("");
        aggregatePM10TimerEvent.fire("");
        aggregatePM2_5TimerEvent.fire("");

        LOGGER.info("aggregateCoarseToMinute() - end");
    }

    @Scheduled(every = "1h")
    void aggregateMinuteToHour() {
        LOGGER.info("aggregateMinuteToHour() - start");

        aggregateMinuteToHourTimerEvent.fire("");

        LOGGER.info("aggregateMinuteToHour() - end");
    }

    @Scheduled(every = "24h")
    void aggregateHourToDay() {
        LOGGER.info("aggregateHourToDay() - start");

        aggregateHourToDayTimerEvent.fire("");

        LOGGER.info("aggregateHourToDay() - end");
    }

}
