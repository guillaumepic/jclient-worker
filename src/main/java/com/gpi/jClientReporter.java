package com.gpi;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class jClientReporter extends jClientGeneric implements Runnable {

    private static final Logger logger = Logger.getLogger(jClientReporter.class);
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSSZ");
    private static final String DEFAULT_VALUE = "UNKNOWN";
    private static final String MEMBERS = "members";

    private MongoDatabase db;

    jClientReporter(MongoClient mc, String dbName, String colName) {
        super(mc, dbName, colName);
        db = m_mongoCli.getDatabase(m_db);
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

    @Override
    public void run() {

        try {
            logReport();
            logger.info("Reporter CollStats checks WC singleton:=> " + getWc().toString());
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
