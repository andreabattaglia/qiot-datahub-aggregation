package org.qiot.covid19.datahub.aggregation.persistence;

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
import com.mongodb.client.model.MergeOptions;
import com.mongodb.client.model.MergeOptions.WhenMatched;
import com.mongodb.client.model.MergeOptions.WhenNotMatched;

@ApplicationScoped
public class CoarsePollutionRepository {

    @ConfigProperty(name = "qiot.database.name")
    String DATABASE_NAME;

    @ConfigProperty(name = "qiot.measurement.grain.coarse.pollution.name")
    String COLLECTION_NAME;

    @ConfigProperty(name = "qiot.measurement.grain.minute.collection-name")
    String MERGE_COLLECTION_NAME;

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

    public void aggregatePM2_5(Long minutes) {
        aggregate("pm2_5", minutes);
    }

    public void aggregatePM10(Long minutes) {
        aggregate("pm10", minutes);
    }

    void aggregate(String specie, Long minutes) {
        LOGGER.debug("aggregate(String specie={}) - start", specie);

        collection.aggregate(//
                Arrays.asList(//
                        match(minutes), //
                        group(specie), //
                        project(specie), //
                        sort(), //
                        merge() //
                )//
        ).toCollection();

        LOGGER.debug("aggregate(String specie={}) - end", specie);
    }

    private Bson match(Long minutes) {
        OffsetDateTime utc = OffsetDateTime.now(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.MINUTES);
        Instant max = utc.minus(1L, ChronoUnit.MINUTES).toInstant();
        LOGGER.info("Instant MAX = {}", max);
        if (minutes == -1L)
            return Aggregates.match(Filters.lt("time", max));
        
        Instant min = utc.minus(minutes, ChronoUnit.MINUTES).toInstant();
        LOGGER.info("Instant MIN = {}", min);

        return Aggregates.match(
                Filters.and(Filters.gte("time", min), Filters.lt("time", max)));
    }

    private Bson group(String specie) {
        String $specie = "$" + specie;

        Document id = new Document("$group", new Document("_id", //
                new Document("year", new Document("$year", "$time"))//
                        .append("month", new Document("$month", "$time"))//
                        .append("day", new Document("$dayOfMonth", "$time"))//
                        .append("hour", new Document("$hour", "$time"))//
                        .append("minute", new Document("$minute", "$time"))//
                        .append("stationId", "$stationId")//
                        .append("specie", specie)//
        )//
         // .append("stationId", new Document("$min", "$stationId"))//
                .append("time", new Document("$max", "$time"))//
                .append("min", new Document("$min", $specie))//
                .append("max", new Document("$max", $specie))//
                .append("avg", new Document("$avg", $specie))//
                .append("count", new Document("$sum", 1))//

        );
        return id;
    }

    private Bson project(String specie) {
        return Aggregates.project(fields(excludeId()
        // ,computed("year", new Document("$year", "$time"))
        // ,computed("month", new Document("$month", "$time"))//
        // , computed("day", new Document("$dayOfMonth", "$time"))//
        // , computed("hour", new Document("$hour", "$time"))//
        // , computed("minute", new Document("$minute", "$time"))//
                , computed("stationId", "$_id.stationId")//
                , computed("specie", specie)//
                , include("time", "min", "max", "avg", "count")

        ));
    }

    private Bson sort() {
        return Aggregates.sort(orderBy(descending("time"),
                ascending("stationId"), ascending("specie")));
    }

    private Bson merge() {
        MergeOptions mergeOptions = new MergeOptions()
                .uniqueIdentifier(Arrays.asList("time", "stationId", "specie"))
                .whenMatched(WhenMatched.REPLACE)
                .whenNotMatched(WhenNotMatched.INSERT);
        return Aggregates.merge(MERGE_COLLECTION_NAME, mergeOptions);
    }
}
