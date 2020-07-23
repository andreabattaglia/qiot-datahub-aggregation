package com.redhat.qiot.datahub.aggregation.domain.measurement.day;

import org.bson.codecs.pojo.annotations.BsonId;

public class MeasurementByDay {
    @BsonId
    public MeasurementByDayId id;
    public double min;
    public double max;
    public double avg;
    public int count;
}
