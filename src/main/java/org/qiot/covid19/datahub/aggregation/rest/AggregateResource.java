package org.qiot.covid19.datahub.aggregation.rest;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.qiot.covid19.datahub.aggregation.service.OnDemandService;
import org.slf4j.Logger;

@ApplicationScoped
@Produces(MediaType.TEXT_PLAIN)
@Consumes(MediaType.TEXT_PLAIN)
@Path("/aggregate")
public class AggregateResource {

    @Inject
    Logger LOGGER;

    @Inject
    OnDemandService onDemandService;

    @GET
    public void run(@QueryParam("grain") AggregationGrainType grain) {
        switch (grain) {
        case minute:
            onDemandService.aggregateCoarseToMinute();
            break;
        case hour:
            onDemandService.aggregateMinuteToHour();
            break;
        case day:
            onDemandService.aggregateHourToDay();
            break;
        case month:
            onDemandService.aggregateDayToMonth();
            break;
        case all:
            onDemandService.aggregateAll();
        default:
            throw new RuntimeException("Requested grain is invalid: " + grain);
        }
    }

}