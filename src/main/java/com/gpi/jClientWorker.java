package com.gpi;

import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import org.apache.log4j.Logger;
import org.bson.Document;
import uk.dioxic.mgenerate.core.Template;

import java.util.ArrayList;
import java.util.Arrays;

import org.bson.Document;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.function.Consumer;

import static java.lang.Thread.sleep;

public class jClientWorker extends jClientGeneric implements Runnable {

    private static final Logger logger = Logger.getLogger(jClientReporter.class);
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSSZ");
    private MongoDatabase db;
    private String m_jsonModelName;
    private Boolean m_isUp = true;
    private int m_bulkSize = 10;

    jClientWorker(MongoClient mc, String dbName, String colName, String jsonModelName) {
        super(mc, dbName, colName);
        db = m_mongoCli.getDatabase(m_db);
        m_jsonModelName = jsonModelName;
        if (getClass().getClassLoader().getResource(m_jsonModelName) == null) {
            logger.warn("JClientWorker doesn't have a model resource. json model not found");
            m_isUp = false;
        }
    }

    public Boolean get_isUp() {
        return m_isUp;
    }

    Consumer<Document> printConsumer = new Consumer<Document>() {
        @Override
        public void accept(final Document document) {
            System.out.println(document.toJson());
        }
    };

    private void doAggregateOne() {

        MongoCollection<Document> collection = m_mongoCli
                .getDatabase(m_db)
                .getCollection(m_col).withWriteConcern(WriteConcern.MAJORITY);

        try {
            AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
                    new Document("$match",
                            new Document("buId", 1L)
                                    .append("contextIdentifiers",
                                            new Document("type", "ACCOUNT")
                                                    .append("value", "12345678"))),
                    new Document("$lookup",
                            new Document("from", "counterType")
                                    .append("let",
                                            new Document("t", "$type.$id"))
                                    .append("pipeline", Arrays.asList(new Document("$match",
                                                    new Document("value", "E")),
                                            new Document("$match",
                                                    new Document("$expr",
                                                            new Document("$eq", Arrays.asList("$_id", "$$t")))),
                                            new Document("$project",
                                                    new Document("_id", 0L)
                                                            .append("valueCounterType", "$value"))))
                                    .append("as", "res")),
                    new Document("$replaceRoot",
                            new Document("newRoot",
                                    new Document("$mergeObjects", Arrays.asList(new Document("$arrayElemAt", Arrays.asList("$res", 0L)), "$$ROOT")))),
                    new Document("$project",
                            new Document("_id", 1L)
                                    .append("valueCounter", "$value")
                                    .append("earnValue", 1L)
                                    .append("valueCounterType", 1L)),
                    new Document("$match",
                            new Document("valueCounterType", "E"))));


            // Print out simple
            for (org.bson.Document document : result) {
                String json = document.toJson();
                logger.info(json);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void doInsertMany() {

        MongoCollection<Template> collection = m_mongoCli
                .getDatabase(m_db)
                .getCollection(m_col, Template.class).withWriteConcern(WriteConcern.MAJORITY);

        try {
            logger.info("Trial aggregation");

            ClassLoader classLoader = getClass().getClassLoader();
            String JsonModelPrimary = classLoader.getResource(m_jsonModelName).getPath();
            // Template template;
            List<Template> documents = new ArrayList<>();
            for (int i = 0; i < m_bulkSize; i++) {
                //documents.add(Template.from(JsonModelPrimary));
                Template template = Template.from(JsonModelPrimary);
                collection.insertOne(template);

                Document doc = template.getDocument();
                if (doc.containsKey("jClientArray")) {
                    List<Document> outerItems = null;
                    logger.info("Good we are !");
                    Document x = doc.get("jClientArray", doc.getClass());
                }
                //}
                //collection.insertMany(documents);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

        public void run() {
        try {
            //doInsertMany();
            doAggregateOne();
            sleep(10000);
        } catch (Exception e) {
            System.out.println("Error jClientWorker: " + e.getMessage());
        }
    }
}