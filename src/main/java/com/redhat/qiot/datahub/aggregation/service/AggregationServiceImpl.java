package com.redhat.qiot.datahub.aggregation.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import com.redhat.qiot.datahub.aggregation.domain.measurement.hour.MeasurementByHour;
import com.redhat.qiot.datahub.aggregation.domain.measurement.minute.MeasurementByMinute;
import com.redhat.qiot.datahub.aggregation.persistence.CoarseGasRepository;
import com.redhat.qiot.datahub.aggregation.persistence.CoarsePollutionRepository;
import com.redhat.qiot.datahub.aggregation.persistence.MeasurementByDayRepository;
import com.redhat.qiot.datahub.aggregation.persistence.MeasurementByHourRepository;
import com.redhat.qiot.datahub.aggregation.persistence.MeasurementByMinuteRepository;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateHourToDayTimer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateMinuteToHourTimer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateNH3Timer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregateOxidisingTimer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregatePM10Timer;
import com.redhat.qiot.datahub.aggregation.util.events.AggregatePM2_5Timer;

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

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregateNH3(@Observes @AggregateNH3Timer Long value) {
        LOGGER.info("aggregateNH3 - start", value);
        coarseGasRepository.aggregateNH3();
        LOGGER.info("aggregateNH3 - end", value);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregateOxidising(@Observes @AggregateOxidisingTimer Long value) {
        LOGGER.info("aggregateOxidising - start", value);
        coarseGasRepository.aggregateOxidising();
        LOGGER.info("aggregateOxidising - end", value);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregatePM10(@Observes @AggregatePM10Timer Long value) {
        LOGGER.info("aggregatePM10 - start", value);
        coarsePollutionRepository.aggregatePM10();
        LOGGER.info("aggregatePM10 - end", value);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregatePM2_5(@Observes @AggregatePM2_5Timer Long value) {
        LOGGER.info("aggregatePM2_5 - start", value);
        coarsePollutionRepository.aggregatePM2_5();
        LOGGER.info("aggregatePM2_5 - end", value);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregateMinuteToHour(@Observes @AggregateMinuteToHourTimer Long value) {
        LOGGER.info("aggregateMinuteToHour - start", value);
        byMinuteRepository.aggregate(value);
        LOGGER.info("aggregateMinuteToHour - end", value);
    }

    /**
     * @param coordinates
     *            the coordinates to set
     */
    void aggregateHourToDay(@Observes @AggregateHourToDayTimer Long value) {
        LOGGER.info("aggregateHourToDay - start", value);
        byHourRepository.aggregate(value);
        byDayRepository.aggregate();
        LOGGER.info("aggregateHourToDay - end", value);
    }


}
