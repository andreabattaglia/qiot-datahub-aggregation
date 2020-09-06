package com.redhat.qiot.datahub.aggregation.persistence;

import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
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
import com.mongodb.client.model.MergeOptions;
import com.mongodb.client.model.MergeOptions.WhenMatched;
import com.mongodb.client.model.MergeOptions.WhenNotMatched;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class MeasurementByHourRepository {

    @ConfigProperty(name = "qiot.database.name")
    String DATABASE_NAME;

    @ConfigProperty(name = "qiot.measurement.grain.hour.collection-name")
    String COLLECTION_NAME;

    @ConfigProperty(name = "qiot.measurement.grain.hour.ttl-value")
    String TTL;

    @ConfigProperty(name = "qiot.measurement.grain.hour.time-unit")
    String TIME_UNIT;

    @ConfigProperty(name = "qiot.measurement.grain.day.collection-name")
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
        collection.createIndex(Indexes.descending("time"),
                expirationOptionIndex);
        IndexOptions uniqueIndexOptions = new IndexOptions().unique(true);
        collection.createIndex(
                Indexes.descending("time", "stationId", "specie"),
                uniqueIndexOptions);
    }

    public void aggregate(Long days) {
        LOGGER.info("aggregate(Long pastDays={}) - start", days);

        collection.aggregate(//
                Arrays.asList(//
                        match(days), //
                        group(), //
                        project(), //
                        sort(), //
                        merge() //
                )//
        ).toCollection();

        LOGGER.info("aggregate(Long pastDays={}) - end", days);
    }

    private Bson match(Long days) {
        OffsetDateTime utc = OffsetDateTime.now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.DAYS);
        Instant max = utc.minus(0L, ChronoUnit.DAYS).toInstant();
        LOGGER.info("Instant MAX = {}", max);
        if (days == -1L)
            return Aggregates.match(Filters.lt("time", max));
        
        Instant min = utc.minus(days, ChronoUnit.DAYS).toInstant();
        LOGGER.info("Instant MIN = {}", min);
        return Aggregates.match(
                Filters.and(Filters.gte("time", min), Filters.lt("time", max)));
    }

    private Bson group() {
        Document id = new Document("$group", new Document("_id", //
                new Document("year", new Document("$year", "$time"))//
                        .append("month", new Document("$month", "$time"))//
                        .append("day", new Document("$dayOfMonth", "$time"))//
                        .append("stationId", "$stationId")//
                        .append("specie", "$specie")//
        )//
                .append("time", new Document("$max", "$time"))//
                .append("min", new Document("$min", "$min"))//
                .append("max", new Document("$max", "$max"))//
                .append("avg", new Document("$avg", "$avg"))//
                .append("count", new Document("$sum", 1))//

        );
        return id;
    }

    private Bson project() {
        return Aggregates.project(fields(excludeId()
//                ,computed("year", new Document("$year", "$time"))
//                ,computed("month", new Document("$month", "$time"))//
//                , computed("day", new Document("$dayOfMonth", "$time"))//
//                , computed("hour", new Document("$hour", "$time"))//
                , include("time", "min", "max", "avg", "count")//
                , computed("stationId", "$_id.stationId")//
                , computed("specie", "$_id.specie")//

        ));
    }

    private Bson sort() {
        return Aggregates.sort(orderBy(descending("time"),ascending("stationId"),ascending("specie")));
    }

    private Bson merge() {
        MergeOptions mergeOptions = new MergeOptions()
                .uniqueIdentifier(Arrays.asList("time", "stationId", "specie"))
                .whenMatched(WhenMatched.REPLACE)
                .whenNotMatched(WhenNotMatched.INSERT);
        return Aggregates.merge(MERGE_COLLECTION_NAME, mergeOptions);
    }

}
