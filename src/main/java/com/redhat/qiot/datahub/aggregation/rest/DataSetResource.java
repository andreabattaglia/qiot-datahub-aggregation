//package com.redhat.qiot.datahub.aggregation.rest;
//
//import javax.ws.rs.Consumes;
//import javax.ws.rs.GET;
//import javax.ws.rs.Path;
//import javax.ws.rs.Produces;
//import org.jboss.resteasy.annotations.jaxrs.QueryParam;
//import javax.ws.rs.core.MediaType;
//
//@Path("/dataset")
//public class DataSetResource {
//
//    @GET
//    @Produces(MediaType.TEXT_PLAIN)
//    public String getDatasets() {
//        return "hello";
//    }
//
//    @GET
//    @Consumes(MediaType.TEXT_PLAIN)
//    @Produces(MediaType.TEXT_PLAIN)
//    public String getDatasetByStationId(
//            @QueryParam("stationId") String stationId) {
//        return "hello";
//    }
//
//    @GET
//    @Consumes(MediaType.TEXT_PLAIN)
//    @Produces(MediaType.TEXT_PLAIN)
//    public String getDatasetBySerial(@QueryParam("serial") String serial) {
//        return "hello";
//    }
//}