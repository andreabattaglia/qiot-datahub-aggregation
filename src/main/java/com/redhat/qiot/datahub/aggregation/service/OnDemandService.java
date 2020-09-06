package com.redhat.qiot.datahub.aggregation.service;

public interface OnDemandService {

    void aggregateCoarseToMinute();

    void aggregateMinuteToHour();

    void aggregateHourToDay();

    void aggregateAll();

}
