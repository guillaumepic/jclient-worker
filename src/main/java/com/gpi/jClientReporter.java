package com.gpi;

//import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.*;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class jClientReporter extends jClientGeneric implements Runnable {

    private static final Logger logger = Logger.getLogger(jClientReporter.class);
    private MongoDatabase db;
    private MongoCollection<Document> col;

    jClientReporter(MongoClient mc, String dbName, String colName) {
        super(mc, dbName, colName);
        db = m_mongoCli.getDatabase(m_db);
        col = db.getCollection(colName);
    }

    private void logReport() {

        Document collStatsResults = db.runCommand(new Document("collStats", m_col));

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        StringBuilder printStatus = new StringBuilder()
                .append("Collection stats at " + dateFormat.format(date))
                .append(" | ns - " + collStatsResults.get("ns"))
                .append(" | size - " + collStatsResults.get("size"))
                .append(" | count - " + collStatsResults.get("count"))
                .append(" | storageSize - " + collStatsResults.get("storageSize"));
        logger.info(printStatus.toString());
    }

    private void doSomethingCRUD()
    {
        try {
            logger.info("Dummy CRUD updateOne touch counting ");
            col.updateOne(
                    eq("_id", new String("digiposte")),
                    combine(inc("touchCount", 1),
                            currentDate("lastModified")),
                    new UpdateOptions().upsert(true));
        } catch(MongoException e){
            logger.error(e.toString());
        }
    }

    @Override
    public void run() {

        try {
            logReport();
            logger.info("Reporter CollStats checks WC singleton:=> " + getWc().toString());
            doSomethingCRUD();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Output a final summary
     */
    public void finalReport() {
        logger.info("Finalize jClientReporter()");
    }
}
