package org.qiot.covid19.datahub.aggregation.service;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.qiot.covid19.datahub.aggregation.persistence.CoarseGasRepository;
import org.qiot.covid19.datahub.aggregation.persistence.CoarsePollutionRepository;
import org.qiot.covid19.datahub.aggregation.persistence.MeasurementByDayRepository;
import org.qiot.covid19.datahub.aggregation.persistence.MeasurementByHourRepository;
import org.qiot.covid19.datahub.aggregation.persistence.MeasurementByMinuteRepository;
import org.qiot.covid19.datahub.aggregation.util.events.AggregateAll;
import org.qiot.covid19.datahub.aggregation.util.events.AggregateDayToMonthTimer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregateHourToDayTimer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregateMinuteToHourTimer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregateNH3Timer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregateOxidisingTimer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregatePM10Timer;
import org.qiot.covid19.datahub.aggregation.util.events.AggregatePM2_5Timer;
import org.slf4j.Logger;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class AggregationServiceImpl implements AggregationService {
    /**
     * Logger for this class
     */
    @Inject
    Logger LOGGER;

    @Inject
    CoarseGasRepository coarseGasRepository;
    @Inject
    CoarsePollutionRepository coarsePollutionRepository;

    @Inject
    MeasurementByMinuteRepository byMinuteRepository;

    @Inject
    MeasurementByHourRepository byHourRepository;

    @Inject
    MeasurementByDayRepository byDayRepository;

    void onStart(@Observes StartupEvent ev) {
        LOGGER.info("Running aggregation pipelines once at startup...");
        runAggregationOnce(-1L);
    }
    
     void runAggregationOnce(@Observes @AggregateAll Long value) {
         LOGGER.info("Aggregating all the available data without time offsets...");
        coarseGasRepository.aggregateNH3(-1L);
        coarseGasRepository.aggregateOxidising(-1L);
        coarsePollutionRepository.aggregatePM10(-1L);
        coarsePollutionRepository.aggregatePM2_5(-1L);
        byMinuteRepository.aggregate(-1L);
        byHourRepository.aggregate(-1L);
        byDayRepository.aggregate();
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregateNH3(@Observes @AggregateNH3Timer Long minutes) {
        LOGGER.info("aggregateNH3 - start", minutes);
        coarseGasRepository.aggregateNH3(minutes);
        LOGGER.info("aggregateNH3 - end", minutes);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregateOxidising(@Observes @AggregateOxidisingTimer Long minutes) {
        LOGGER.info("aggregateOxidising - start", minutes);
        coarseGasRepository.aggregateOxidising(minutes);
        LOGGER.info("aggregateOxidising - end", minutes);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregatePM10(@Observes @AggregatePM10Timer Long minutes) {
        LOGGER.info("aggregatePM10 - start", minutes);
        coarsePollutionRepository.aggregatePM10(minutes);
        LOGGER.info("aggregatePM10 - end", minutes);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregatePM2_5(@Observes @AggregatePM2_5Timer Long minutes) {
        LOGGER.info("aggregatePM2_5 - start", minutes);
        coarsePollutionRepository.aggregatePM2_5(minutes);
        LOGGER.info("aggregatePM2_5 - end", minutes);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregateMinuteToHour(
            @Observes @AggregateMinuteToHourTimer Long hours) {
        LOGGER.info("aggregateMinuteToHour - start", hours);
        byMinuteRepository.aggregate(hours);
        LOGGER.info("aggregateMinuteToHour - end", hours);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregateHourToDay(@Observes @AggregateHourToDayTimer Long days) {
        LOGGER.info("aggregateHourToDay - start", days);
        byHourRepository.aggregate(days);
        byDayRepository.aggregate();
        LOGGER.info("aggregateHourToDay - end", days);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregateDayToMonth(@Observes @AggregateDayToMonthTimer Long days) {
        LOGGER.info("aggregateHourToDay - start", days);
        byDayRepository.aggregate();
        LOGGER.info("aggregateHourToDay - end", days);
    }

}
