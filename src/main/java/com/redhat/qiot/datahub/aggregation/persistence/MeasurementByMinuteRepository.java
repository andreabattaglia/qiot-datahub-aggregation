package com.redhat.qiot.datahub.aggregation.persistence;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
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
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class MeasurementByMinuteRepository {

    @ConfigProperty(name = "qiot.database.name")
    String DATABASE_NAME;

    @ConfigProperty(name = "qiot.measurement.grain.minute.collection-name")
    String COLLECTION_NAME;

    @ConfigProperty(name = "qiot.measurement.grain.minute.ttl-value")
    String TTL;

    @ConfigProperty(name = "qiot.measurement.grain.minute.time-unit")
    String TIME_UNIT;

    @ConfigProperty(name = "qiot.measurement.grain.hour.collection-name")
    String MERGE_COLLECTION_NAME;

    @Inject
    Logger LOGGER;

    @Inject
    MongoClient mongoClient;

    MongoDatabase qiotDatabase = null;
    MongoCollection<Document> collection = null;
    CodecProvider pojoCodecProvider = null;
    CodecRegistry pojoCodecRegistry = null;

    void onStart(@Observes StartupEvent ev) {
    }

    @PostConstruct
    void init() {
        qiotDatabase = mongoClient.getDatabase(DATABASE_NAME);
        try {
            qiotDatabase.createCollection(COLLECTION_NAME);
        } catch (Exception e) {
            LOGGER.info("Collection {} already exists", COLLECTION_NAME);
        }
        collection = qiotDatabase.getCollection(COLLECTION_NAME);

        /*
         * ensure indexes exist
         */
        ensureIndexes();

        // Create a CodecRegistry containing the PojoCodecProvider instance.
        pojoCodecProvider = PojoCodecProvider.builder()
                .register("com.mongodb.client.model").automatic(true).build();
        pojoCodecRegistry = CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(pojoCodecProvider));
        collection = collection.withCodecRegistry(pojoCodecRegistry);
    }

    private void ensureIndexes() {
        LOGGER.info("Setting TTL for {}: time to live={}, timeUnit={}",
                COLLECTION_NAME, TTL, TIME_UNIT);
        IndexOptions expirationOptionIndex = new IndexOptions()
                .expireAfter(Long.parseLong(TTL), TimeUnit.valueOf(TIME_UNIT));
        collection.createIndex(Indexes.ascending("time"),
                expirationOptionIndex);
    }

    public void aggregate() {
        collection.aggregate(Arrays.asList(//
                match(), //
                group()
                , Aggregates.merge(MERGE_COLLECTION_NAME)))
                .forEach((d) -> LOGGER.trace("DOCUMENT RESULT {}",
                        d.toString()));
    }

    private Bson match() {
        OffsetDateTime utc = OffsetDateTime.now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.HOURS);
        Date min = Date.from(utc.minus(1L, ChronoUnit.HOURS).toInstant());
        LOGGER.debug("Date MIN = {}", min);
        Date max = Date.from(utc.minus(0L, ChronoUnit.HOURS).toInstant());
        LOGGER.debug("Date MAX = {}", max);

        return Aggregates.match(
                Filters.and(Filters.gte("time", min), Filters.lt("time", max)));
    }

    private Bson group() {
        Document id = new Document("$group", new Document("_id", //
                new Document("year", "$_id.year")//
                        .append("month", "$_id.month")//
                        .append("day", "$_id.day")//
                        .append("hour", "$_id.hour")//
                        .append("stationId", "$_id.stationId")//
                        .append("specie", "$_id.specie")//
        )//
                .append("time", new Document("$max", "$time"))//
                .append("min", new Document("$min", "$min"))//
                .append("max", new Document("$max", "$max"))//
                .append("avg", new Document("$avg", "$avg"))//
                .append("count", new Document("$sum", 1))//

        );
        return id;
    }

}
