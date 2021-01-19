package org.qiot.covid19.datahub.aggregation.service;

public interface OnDemandService {

    void aggregateCoarseToMinute();

    void aggregateMinuteToHour();

    void aggregateHourToDay();

    void aggregateDayToMonth();

    void aggregateAll();

}
