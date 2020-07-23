package com.redhat.qiot.datahub.aggregation.domain.measurement;

import java.util.Date;

import io.quarkus.runtime.annotations.RegisterForReflection;

/*
 {
"stationId":int,
"instant":long,
"PM1_0":int,
"PM2_5":int,
"PM10":int,
"PM1_0_atm":int,
“PM2_5_atm":int,
"PM10_atm":int,
"gt0_3um":int,
"gt0_5um":int,
"gt1_0um":int,
"gt2_5um":int,
"gt5_0um":int
"gt10um":int
}
 */
@RegisterForReflection
public class PollutionMeasurement {
    public int stationId;
    public Date time;
    public int pm1_0;
    public int pm2_5;
    public int pm10;
    public int pm1_0_atm;
    public int pm2_5_atm;
    public int pm10_atm;
    public int gt0_3um;
    public int gt0_5um;
    public int gt1_0um;
    public int gt2_5um;
    public int gt5_0um;
    public int gt10um;

    @Override
    public String toString() {
        return "PollutionMeasurement [stationId=" + stationId + ", time=" + time
                + ", pm1_0=" + pm1_0 + ", pm2_5=" + pm2_5 + ", pm10=" + pm10
                + ", pm1_0_atm=" + pm1_0_atm + ", pm2_5_atm=" + pm2_5_atm
                + ", pm10_atm=" + pm10_atm + ", gt0_3um=" + gt0_3um
                + ", gt0_5um=" + gt0_5um + ", gt1_0um=" + gt1_0um + ", gt2_5um="
                + gt2_5um + ", gt5_0um=" + gt5_0um + ", gt10um=" + gt10um + "]";
    }

}
