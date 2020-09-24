/**
 * 
 */
package com.redhat.qiot.datahub.aggregation.service;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;

import com.redhat.qiot.datahub.aggregation.util.events.AggregateAll;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateDayToMonthTimer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateHourToDayTimer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateMinuteToHourTimer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateNH3Timer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateOxidisingTimer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregatePM10Timer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregatePM2_5Timer;

/**
 * @author abattagl
 *
 */
@ApplicationScoped
public class OnDemandServiceImpl implements OnDemandService {
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
    @AggregateDayToMonthTimer
    Event<Long> aggregateDayToMonthTimerEvent;

    @Inject
    @AggregateAll
    Event<Long> aggregateAllEvent;

    @Override
    public void aggregateCoarseToMinute() {
        LOGGER.info("aggregateCoarseToMinute() - start");

        long minTime = -1;
        aggregateNH3TimerEvent.fire(minTime);
        aggregateOxidisingTimerEvent.fire(minTime);
        aggregatePM10TimerEvent.fire(minTime);
        aggregatePM2_5TimerEvent.fire(minTime);

        LOGGER.info("aggregateCoarseToMinute() - end");
    }

    @Override
    public void aggregateMinuteToHour() {
        LOGGER.info("aggregateMinuteToHour() - start");

        aggregateMinuteToHourTimerEvent.fire(-1L);

        LOGGER.info("aggregateMinuteToHour() - end");
    }

    @Override
    public void aggregateHourToDay() {
        LOGGER.info("aggregateHourToDay() - start");

        aggregateHourToDayTimerEvent.fire(-1L);

        LOGGER.info("aggregateHourToDay() - end");
    }

    @Override
    public void aggregateDayToMonth() {
        LOGGER.info("aggregateHourToDay() - start");

        aggregateDayToMonthTimerEvent.fire(-1L);

        LOGGER.info("aggregateHourToDay() - end");
    }

    @Override
    public void aggregateAll() {
        aggregateAllEvent.fire(-1L);
    }
}
