package com.redhat.qiot.datahub.aggregation.persistence;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.bson.Document;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

@ApplicationScoped
public class CoarsePollutionRepository {

    @ConfigProperty(name = "qiot.database.name")
    String DATABASE_NAME;

    @ConfigProperty(name = "qiot.measurement.grain.coarse.pollution.name")
    String COLLECTION_NAME;

    @Inject
    Logger LOGGER;

    @Inject
    MongoClient mongoClient;

    MongoDatabase qiotDatabase;
    MongoCollection<Document> collection = null;
    CodecProvider codecProvider;
    CodecRegistry codecRegistry;

    // void onStart(@Observes StartupEvent ev) {
    // }

    @PostConstruct
    void init() {
        qiotDatabase = mongoClient.getDatabase(DATABASE_NAME);
        // try {
        // qiotDatabase.createCollection(COLLECTION_NAME);
        // } catch (Exception e) {
        // LOGGER.debug("Collection {} already exists", COLLECTION_NAME);
        // }
        collection = qiotDatabase.getCollection(COLLECTION_NAME);

        // Create a CodecRegistry containing the PojoCodecProvider instance.
        codecProvider = PojoCodecProvider.builder()
                .register("com.mongodb.client.model").automatic(true).build();
        codecRegistry = CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(codecProvider));
        collection = collection.withCodecRegistry(codecRegistry);
    }

    public void aggregatePM2_5() {
        aggregate("pm2_5");
    }

    public void aggregatePM10() {
        aggregate("pm10");
    }

    void aggregate(String specie) {
        collection.aggregate(//
                Arrays.asList(//
                        match(), //
                        group(specie), //
                        Aggregates.merge("measurementbyminute")//
                )//
        ).forEach((d) -> LOGGER.trace("DOCUMENT RESULT {}", d.toString()));
    }

    private Bson match() {

        OffsetDateTime utc = OffsetDateTime.now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.MINUTES);
        Date min = Date.from(utc.minus(2L, ChronoUnit.MINUTES).toInstant());
        LOGGER.info("Date MIN = {}", min);
        Date max = Date.from(utc.minus(1L, ChronoUnit.MINUTES).toInstant());
        LOGGER.info("Date MAX = {}", max);

        return Aggregates.match(
                Filters.and(Filters.gte("time", min), Filters.lt("time", max)));
    }

    private Bson group(String specie) {
        String $specie = "$" + specie;
        Document id = new Document("$group", //
                new Document("_id", //
                        new Document("year", new Document("$year", "$time"))//
                                .append("month",
                                        new Document("$month", "$time"))//
                                .append("day",
                                        new Document("$dayOfMonth", "$time"))//
                                .append("hour", new Document("$hour", "$time"))//
                                .append("minute",
                                        new Document("$minute", "$time"))//
                                .append("stationId", "$stationId")//
                                .append("specie", specie)//
                )//
                        .append("time", new Document("$max", "$time"))//
                        .append("min", new Document("$min", $specie))//
                        .append("max", new Document("$max", $specie))//
                        .append("avg", new Document("$avg", $specie))//
                        .append("count", new Document("$sum", 1))//
        );
        return id;
    }

    // private Bson sort() {
    // Document sort = new Document("$sort", new Document("_id", 1));
    // return sort;
    // }
}
