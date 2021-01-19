package org.qiot.covid19.datahub.aggregation.persistence;

import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;

import java.util.Arrays;

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
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.MergeOptions;
import com.mongodb.client.model.MergeOptions.WhenMatched;
import com.mongodb.client.model.MergeOptions.WhenNotMatched;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class MeasurementByDayRepository {

    @ConfigProperty(name = "qiot.database.name")
    String DATABASE_NAME;

    @ConfigProperty(name = "qiot.measurement.grain.day.collection-name")
    String COLLECTION_NAME;

    @ConfigProperty(name = "qiot.measurement.grain.month.collection-name")
    String MERGE_COLLECTION_NAME;

    @Inject
    Logger LOGGER;

    @Inject
    MongoClient mongoClient;

    MongoDatabase qiotDatabase = null;
    MongoCollection<Document> collection = null;
    CodecProvider pojoCodecProvider = null;
    CodecRegistry pojoCodecRegistry = null;

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
        collection.createIndex(Indexes.descending("time"));
        IndexOptions uniqueIndexOptions = new IndexOptions().unique(true);
        collection.createIndex(
                Indexes.descending("time", "stationId", "specie"),
                uniqueIndexOptions);
    }

    public void aggregate() {
        collection.aggregate(//
                Arrays.asList(//
                        group(), //
                        project(), //
                        sort(), //
                        merge() //
                )//
        ).toCollection();
    }

    private Bson group() {
        Document id = new Document("$group", new Document("_id", //
                new Document("year", new Document("$year", "$time"))//
                        .append("month", new Document("$month", "$time"))//
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
        // ,computed("year", new Document("$year", "$time"))
        // ,computed("month", new Document("$month", "$time"))//
        // , computed("day", new Document("$dayOfMonth", "$time"))//
        // , computed("hour", new Document("$hour", "$time"))//
                , include("time", "min", "max", "avg", "count")//
                , computed("stationId", "$_id.stationId")//
                , computed("specie", "$_id.specie")//
                ,
                computed("month",
                        new Document("$concat",
                                Arrays.asList(new Document("$toString", new Document("$year", "$time")),
                                        new Document("$toString", new Document("$month", "$time")))))

        ));
    }

    private Bson sort() {
        return Aggregates.sort(orderBy(descending("time"),
                ascending("stationId"), ascending("specie")));
    }

    private Bson merge() {
        MergeOptions mergeOptions = new MergeOptions()
                .uniqueIdentifier(Arrays.asList("month", "stationId", "specie"))
                .whenMatched(WhenMatched.REPLACE)
                .whenNotMatched(WhenNotMatched.INSERT);
        return Aggregates.merge(MERGE_COLLECTION_NAME, mergeOptions);
    }

}
