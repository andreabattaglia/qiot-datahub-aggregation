package com.redhat.qiot.datahub.aggregation.persistence;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.bson.Document;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class MeasurementByMonthRepository {

    @ConfigProperty(name = "qiot.database.name")
    String DATABASE_NAME;

    @ConfigProperty(name = "qiot.measurement.grain.month.collection-name")
    String COLLECTION_NAME;

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
        collection.createIndex(Indexes.descending("time"));
        IndexOptions uniqueIndexOptions = new IndexOptions().unique(true);
        collection.createIndex(
                Indexes.descending("time", "stationId", "specie"),
                uniqueIndexOptions);
    }

}
