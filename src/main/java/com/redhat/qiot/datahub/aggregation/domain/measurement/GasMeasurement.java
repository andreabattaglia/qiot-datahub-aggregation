package com.redhat.qiot.datahub.aggregation.domain.measurement;

import java.time.Instant;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class GasMeasurement {
    public int stationId;
    public Instant time;
    public Double adc;
    public double nh3;
    public double oxidising;
    public double reducing;

    @Override
    public String toString() {
        return "GasMeasurement [stationId=" + stationId + ", time=" + time
                + ", adc=" + adc + ", nh3=" + nh3 + ", oxidising=" + oxidising
                + ", reducing=" + reducing + "]";
    }

}
