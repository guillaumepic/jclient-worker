package com.gpi;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import org.bson.*;

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

public class jClientRSreporter extends jClientGeneric implements Runnable {

    private static final Logger logger = Logger.getLogger(jClientRSreporter.class);
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSSZ");
    private static final String DEFAULT_VALUE = "UNKNOWN";
    private static final String MEMBERS = "members";
    private static final String CONFIG = "config";
    private int detectedVotes = 0;
    private int instantVotes =  0;

    private MongoDatabase db;

    jClientRSreporter(MongoClient mc, String dbName, String colName) {
        super(mc, dbName, colName);
        m_db = "admin";
        db = m_mongoCli.getDatabase(m_db);
        initRSConfiguration();
    }

    private void initRSConfiguration() {
        try {
            detectedVotes = 0;
            BsonDocument commandResult = db.runCommand(new BsonDocument("replSetGetConfig", new BsonInt32(1)), BsonDocument.class);
            report(commandResult);
            logger.info("Detected voting (vote, not arbiter, not delayed, not hidden) member in the RS configuration is: " + detectedVotes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void report(BsonDocument replicasetStatusDocument) {
        if (isGetStatus(replicasetStatusDocument)) {
            replicasetStatusDocument.getArray(MEMBERS).stream()
                    .filter(BsonValue::isDocument)
                    .map(memberDocument -> (BsonDocument) memberDocument)
                    .forEach(memberDocument -> logMemberDocument(memberDocument));
        } else if (isGetConfiguration(replicasetStatusDocument)) {
            replicasetStatusDocument.getDocument(CONFIG).getArray(MEMBERS).stream()
                    .filter(BsonValue::isDocument)
                    .map(memberDocument -> (BsonDocument) memberDocument)
                    .forEach(memberDocument -> logConfigMemberDocument(memberDocument));
        } else {
            logger.warn("The replicaset status document does not contain a '{}' attributes");
        }
    }

    private boolean isGetStatus(BsonDocument replicasetStatusDocument) {
        return replicasetStatusDocument.containsKey(MEMBERS)
                && replicasetStatusDocument.get(MEMBERS).isArray();
    }

    private boolean isGetConfiguration(BsonDocument replicasetStatusDocument) {
        return replicasetStatusDocument.containsKey(CONFIG)
                && replicasetStatusDocument.getDocument(CONFIG).get(MEMBERS).isArray();
    }

    private void logConfigMemberDocument(BsonDocument memberDocument) {
        StringBuilder stringBuilder = new StringBuilder()
                .append(logAttribute("node", getStringValue(memberDocument, "host")))
                .append(logAttribute("votes", getNumericValue(memberDocument, "votes")))
                .append(logAttribute("arbiterOnly", getBooleanValue(memberDocument, "arbiterOnly")))
                .append(logAttribute("priority", getNumericValue(memberDocument, "priority")))
                .append(logAttribute("hidden", getBooleanValue(memberDocument, "hidden")))
                .append(logAttribute("slaveDelay", getNumericValue(memberDocument, "slaveDelay")));
        logger.info(stringBuilder.toString());

        updateDetectedVotes(memberDocument);
    }

    private void logMemberDocument(BsonDocument memberDocument) {
        StringBuilder stringBuilder = new StringBuilder()
                .append(logAttribute("node", getStringValue(memberDocument, "name")))
                .append(logAttribute("health", getNumericValue(memberDocument, "health")))
                .append(logAttribute("state", getStringValue(memberDocument, "stateStr")))
                .append(logAttribute("uptime(s)", getNumericValue(memberDocument, "uptime")))
                .append(logAttribute("lastOptime", getDateTimeValue(memberDocument, "optimeDate")))
                .append(logAttribute("lastHeartbeat", getDateTimeValue(memberDocument, "lastHeartbeat")))
                .append(logAttribute("lastHeartbeatRecv", getDateTimeValue(memberDocument, "lastHeartbeatRecv")))
                .append(logAttribute("ping(ms)", getNumericValue(memberDocument, "pingMs")))
                .append(logAttribute("replicationLag(s)", getReplicationLag(memberDocument)));

        logger.info(stringBuilder.toString());
        updateInstantVotes(memberDocument);
    }

    private void updateDetectedVotes(BsonDocument memberDocument) {
        if (memberDocument.getNumber("votes").intValue() > 0
                && !memberDocument.getBoolean("arbiterOnly").getValue()
                && !memberDocument.getBoolean("hidden").getValue()
                && memberDocument.getNumber("slaveDelay").intValue() == 0) {
            detectedVotes += 1;
        }
    }

    private void updateInstantVotes(BsonDocument memberDocument) {
        if (memberDocument.getNumber("health").intValue() > 0
                && (memberDocument.getNumber("state").intValue() ==1 || memberDocument.getNumber("state").intValue() ==2)) {
            instantVotes += 1;
        }
    }

    private void updateWriteConcern() {
        logger.info("Detected voting (healthy, secondary or primary ) member in the RS configuration is: " + instantVotes + " compare to total of: " + detectedVotes);
        int w =  (int) Math.floor(detectedVotes/2) + 1;

        logger.info("deduced w majority limit: " + w + " instant votes:" + instantVotes + " WriteConcern applied currently:" + getWc().toString() );
        if ( w > instantVotes ) {
            logger.warn("Yo We've got problem here ... do something");
            setWc(new WriteConcern(w));
        }

        if (instantVotes >= w) {
            logger.info("Things are fine - use majority");
            setWc(WriteConcern.MAJORITY);
        }
    }

    private String logAttribute(String key, Optional<String> value) {
        return new StringBuilder(key).append("=").append(value.orElse(DEFAULT_VALUE)).append("|").toString();
    }

    private Optional<String> getStringValue(BsonDocument memberDocument, String key) {
        if (memberDocument.containsKey(key)) {
            try {
                return Optional.of(memberDocument.getString(key).getValue().toUpperCase());
            } catch (BsonInvalidOperationException e) {
                logger.warn("Exception reading: {} from replica Set status document, message: {}." + key + e.getMessage());
            }
        }
        return Optional.empty();
    }

    private Optional<String> getNumericValue(BsonDocument memberDocument, String key) {
        if (memberDocument.containsKey(key)) {
            BsonNumber bsonNumber = memberDocument.getNumber(key);
            if (bsonNumber.isInt32()) {
                return Optional.of(Integer.toString(bsonNumber.intValue()));
            } else if (bsonNumber.isInt64()) {
                return Optional.of(Long.toString(bsonNumber.longValue()));
            } else if (bsonNumber.isDouble()) {
                return Optional.of(Double.toString(bsonNumber.doubleValue()));
            }
        }
        return Optional.empty();
    }

    private Optional<String> getBooleanValue(BsonDocument memberDocument, String key) {
        if (memberDocument.containsKey(key)) {
            return Optional.of(memberDocument.getBoolean(key, BsonBoolean.FALSE).toString());
        }
        return Optional.empty();
    }

    private Optional<String> getDateTimeValue(BsonDocument memberDocument, String key) {
        if (memberDocument.containsKey(key)) {
            try {
                return Optional.of(dateFormatter.format(new Date(memberDocument.getDateTime(key).getValue())));
            } catch (BsonInvalidOperationException e) {
                logger.warn("Exception reading: {} from replicaset status document due to: {}!" + key + e.getMessage());
            }
        }
        return Optional.empty();
    }

    private Optional<String> getReplicationLag(BsonDocument memberDocument) {
        if (memberDocument.containsKey("optimeDate") && memberDocument.containsKey("lastHeartbeat")) {
            try {
                long optimeDate = memberDocument.getDateTime("optimeDate").getValue();
                long lastHeartbeat = memberDocument.getDateTime("lastHeartbeat").getValue();
                long replicationLag = lastHeartbeat - optimeDate;
                return Optional.of(Long.toString(replicationLag));
            } catch (BsonInvalidOperationException e) {
                logger.warn("Exception reading 'optimeDate' or 'lastHeartbeat' from replicaset status document due to: {}!" + e.getMessage());
            } catch (IllegalArgumentException e) {
                logger.warn("Exception calculating the replication lag due to: {}!" + e.getMessage());
            }
        }
        return Optional.empty();
    }

    @Override
    public void run() {

        try {
            instantVotes = 0 ; // reload countings of voting members
            BsonDocument commandResult = db.runCommand(new BsonDocument("replSetGetStatus", new BsonInt32(1)), BsonDocument.class);
            report(commandResult);
            updateWriteConcern();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}