package com.redhat.qiot.datahub.aggregation.domain.measurement.hour;

import org.bson.codecs.pojo.annotations.BsonId;

public class MeasurementByHour {
    @BsonId
    public MeasurementByHourId id;
    public double min;
    public double max;
    public double avg;
    public int count;

}
