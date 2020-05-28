package com.gpi;

// import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import org.apache.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.conversions.Bson;

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
    private static final int IntervalRange = 1000;

    private MongoDatabase db;
    private MongoCollection<Document> col;
    private String m_colTokens, m_colPricing;
    private int m_rangeId;
    private String m_uuid;

    jClientWatcher(MongoClient mc, String dbName, String colName, int rangeId) {
        super(mc, dbName, colName);

        db = m_mongoCli.getDatabase(m_db);
        col = db.getCollection(m_col);

        // Collection name persisting effective pricing
        m_colPricing = m_col + "_updated";
        // Collection name persisting event change token that are processed
        m_colTokens = m_col + "_token";
        // Initiate unique id for token entry
        m_uuid = getExistingWatcher();
        logger.debug("Associated UUID: " + m_uuid);
        // Dummy partitioning watcher ID
        m_rangeId = rangeId;
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
        // FindOneAndReplaceOptions opt = new FindOneAndReplaceOptions().upsert(true);
        // collection.findOneAndReplace(eq("_id", this.m_uuid), resumeToken, opt);
        ReplaceOptions opt = new ReplaceOptions().upsert(true);
        collection.replaceOne(  eq("_id", this.m_uuid), resumeToken, opt);
    }

    /**
     * Foo update to init offer status
     * @param offerId
     *
     */
    protected void setPricingEntry(BsonNumber offerId) {
        try {
            MongoCollection<Document> collection = db.getCollection(m_colPricing);
            Bson offerEntry = combine( set("dateStatus", new Date()), set("status", new String("Pending")));
            collection.updateOne(eq("offerId", offerId), offerEntry, new UpdateOptions().upsert(true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Foo update to validate offer status
     * @param offerId
     */
    protected void updatePricingEntry(BsonNumber offerId) {
        try {
            MongoCollection<BsonDocument> collection = db.getCollection(m_colPricing, BsonDocument.class);
            Bson offerFilter = combine( eq("offerId",offerId), eq("status", new String("Pending")));
            Bson offerUpdate = combine( set("dateEffective", new Date()), set("status", new String("Effective")));
            collection.updateOne(offerFilter, offerUpdate);
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
            // Note : Range are used to assign specific watcher to specific _id range
            Bson filter = Filters.and(
                    Filters.in("operationType", asList("insert", "delete", "update")),
                    Filters.gte("documentKey._id",m_rangeId),
                    Filters.lt("documentKey._id",m_rangeId + IntervalRange));
            watchCursor = col.watch(asList(Aggregates.match(filter)));
            if (resumeToken!= null) {
                logger.info("Watcher resume token is : " + resumeToken.toString());
                watchCursor.resumeAfter(resumeToken);
            }
            else {
                logger.info("Watcher resume token is null ");
            }

            // watchCursor.maxAwaitTime(maxWaitTime, TimeUnit.MILLISECONDS); // Testing-scaling
            // watchCursor.batchSize(defaultBatchSize); // Testing-scaling
            // watchCursor.startAtOperationTime(new BsonTimestamp(System.currentTimeMillis()-250)); // alternative resume strategy
            watchCursor.fullDocument(true ? FullDocument.UPDATE_LOOKUP : FullDocument.DEFAULT); // warning full document

            MongoCursor<ChangeStreamDocument<Document>> iterator = watchCursor.iterator();
            long start = System.currentTimeMillis();
            while (iterator.hasNext()) {
                ChangeStreamDocument<Document> doc = iterator.next();
                logger.info("Watcher processed event resumeToken: " + doc.getResumeToken());
                logger.info("Watcher oplog content: " + doc.getDocumentKey().toString());
                if (doc.getOperationType() == OperationType.INSERT || doc.getOperationType() == OperationType.UPDATE ) {
                    logger.info("Watcher fulldocument content on insert: " + doc.getFullDocument().toString());
                    setPricingEntry(doc.getDocumentKey().getNumber("_id")); // Warning: ensure idempotent op
                    storeResumeToken(doc.getResumeToken());
                }
                else if (doc.getOperationType() == OperationType.DELETE) {
                    logger.info("Watcher oplog on delete ");
                    updatePricingEntry(doc.getDocumentKey().getNumber("_id")); // Warning: ensure idempotent op
                    storeResumeToken(doc.getResumeToken());
                }
                else{
                    logger.info("Watcher do nothing here ... ");
                }
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
