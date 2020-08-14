package com.redhat.qiot.datahub.aggregation.service;

public interface SchedulerService {

    void aggregateCoarseToMinute(Long min);

    void aggregateMinuteToHour(Long min);

    void aggregateHourToDay(Long min);

}
