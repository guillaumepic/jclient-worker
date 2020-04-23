package com.gpi;

import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.apache.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;

public class jClientWatcher extends jClientGeneric implements Runnable {

    private static final Logger logger = Logger.getLogger(jClientReporter.class);
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSSZ");
    private static final String DEFAULT_VALUE = "UNKNOWN";
    private static final String MEMBERS = "members";

    private MongoDatabase db;
    private MongoCollection<Document> col;
    private String m_colTokens;

    private String m_uuid;

    jClientWatcher(MongoClient mc, String dbName, String colName) {
        super(mc, dbName, colName);

        db = m_mongoCli.getDatabase(m_db);
        col = db.getCollection(m_col);

        // Collection name persisting event change token that are processed
        m_colTokens = m_col + "_token";
        // Initiate unique id for token entry
        m_uuid = getExistingWatcher();
        logger.debug("Associated UUID: " + m_uuid);
    }

    /**
     * Dummy logEvent function
     */
    private void logEvent() {

        Document collStatsResults = db.runCommand(new Document("collStats", m_col));

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        StringBuilder printStatus = new StringBuilder()
                .append("jclient Watcher log event at " + dateFormat.format(date));
        System.out.println(printStatus.toString());
        logger.info(printStatus.toString());
    }

    /**
     * Get the corresponding collection watcher entry in the database for this stream.
     * @return The UUID of the replicator.
     */
    protected String getExistingWatcher() {

        MongoCollection<Document> collection = db.getCollection(m_colTokens);
        collection.createIndex(new Document("uuid", 1));

        FindIterable<Document> replicatorsIterable = collection.find(eq("colWatched",m_col));
        Document replicatorDoc = replicatorsIterable.first();
        if (replicatorDoc != null && replicatorDoc.containsKey("uuid")) {
            return replicatorDoc.getString("uuid");
        } else {
            String uuid = UUID.randomUUID().toString();
            collection.insertOne(new Document().append("uuid", uuid).append("colWatched", m_col));
            return uuid;
        }
    }

    /**
     * Retrieve the resumeToken from resumeTokensColl, based on the uuid
     * @return the resumeToken
     */
    protected BsonDocument getResumeToken() {

        MongoCollection<BsonDocument> collection = db.getCollection(m_colTokens, BsonDocument.class);
        BsonDocument resumeToken = collection.find(eq("_id", this.m_uuid)).projection(excludeId()).first();
        return resumeToken;
    }

    /**
     * Store resume tokens always
     * @param resumeToken
     */
    protected void storeResumeToken(BsonDocument resumeToken) {
        MongoCollection<BsonDocument> collection = db.getCollection(m_colTokens, BsonDocument.class);
        FindOneAndReplaceOptions opt = new FindOneAndReplaceOptions().upsert(true);
        collection.findOneAndReplace(eq("_id", this.m_uuid), resumeToken, opt);
    }

    /**
     * Foo update to validate event process
     * @param id
     */
    protected void updateEntry(BsonObjectId id) {
        try {
            col.updateOne(eq("_id", id), combine(set("checked", true)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Main runnable function for the task executor
     * @param
     */
    @Override
    public void run() {

        try {
            ChangeStreamIterable<Document> watchCursor = null;
            BsonDocument resumeToken = getResumeToken();
            if (resumeToken!= null) {
                logger.info("Watcher resume token is : " + resumeToken.toString());
                watchCursor = col.watch(asList(Aggregates.match(Filters.in("operationType", asList("insert", "delete")))))
                        .resumeAfter(resumeToken);
            }
            else {
                logger.info("Watcher resume token is null ");
                watchCursor = col.watch(asList(Aggregates.match(Filters.in("operationType", asList("insert", "delete")))));
            }

            // watchCursor.maxAwaitTime(maxWaitTime, TimeUnit.MILLISECONDS);
            // watchCursor.batchSize(defaultBatchSize);
            // watchCursor.startAtOperationTime(new BsonTimestamp(System.currentTimeMillis()-250));
            watchCursor.fullDocument(true ? FullDocument.UPDATE_LOOKUP : FullDocument.DEFAULT);

            MongoCursor<ChangeStreamDocument<Document>> iterator = watchCursor.iterator();
            long start = System.currentTimeMillis();
            while (iterator.hasNext()) {
                logEvent();
                ChangeStreamDocument<Document> doc = iterator.next();
                logger.info("Watcher oplog content on insert: " + doc.getDocumentKey().toString());
                logger.info("Watcher fulldocument content ont insert: " + doc.getFullDocument().toString());
                logger.info("Watcher processed event resumeToken: " + doc.getResumeToken());
                updateEntry(doc.getDocumentKey().getObjectId("_id")); // Assume idempotency for the last update. In case of watch interruption this last update may be applied twice.
                storeResumeToken(doc.getResumeToken());
            }
            iterator.close();
            logger.info("iterator empty");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Output a final summary
     */
    public void finalReport() {
        System.out.println("Finalize jClientWatcher()");
    }

}
