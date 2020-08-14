package com.redhat.qiot.datahub.aggregation.rest;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;

import com.redhat.qiot.datahub.aggregation.service.SchedulerService;

@ApplicationScoped
@Produces(MediaType.TEXT_PLAIN)
@Consumes(MediaType.TEXT_PLAIN)
@Path("/aggregate")
public class AggregateResource {

    @Inject
    Logger LOGGER;
    
    @Inject
    SchedulerService schedulerService;

    @GET
    public void test(@QueryParam("grain") AggregationGrainType grain) {
        switch (grain) {
        case minute:
            schedulerService.aggregateCoarseToMinute(-1L);
            break;
        case hour:
            schedulerService.aggregateMinuteToHour(-1L);
            break;
        case day:
            schedulerService.aggregateHourToDay(-1L);
            break;

        default:
            throw new RuntimeException("Requested grain is invalid: "+grain);
        }
    }

}