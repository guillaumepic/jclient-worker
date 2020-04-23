package com.gpi;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.*;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

/**
 * jClient
 * Main class lancher for the worker pools
 */
public class jClient
{

    private MongoClient m_mongoClient;
    private String m_db, m_col;
    static long start, end;
    static int m_threadCount = 2;
    static long T = 10;
    private static int count=0;

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(jClient.class);
    JsonWriterSettings prettyPrint = JsonWriterSettings.builder().indent(true).build();

    jClient(String conStr, String dbStr, String colStr, int period){

        logger.info( "jClient connection string: " + conStr );
        m_db = dbStr;
        m_col = colStr;
        T = (long) period;
        try {
             // Alt connection string
             m_mongoClient = new MongoClient(new MongoClientURI(conStr));
        }
        catch(Exception e)
            {
                e.printStackTrace();
            }
    }

    /**
     * Dummy method to create an existing, clean with a session document
     * * @param
     */
    public void initSession() {
        try {
            MongoDatabase db = m_mongoClient.getDatabase(m_db);

            // Clean up collection
            db.getCollection(m_col).drop();

            // Create session entry
            Bson sessionFilter = eq("session", new String("jclient"));
            Bson sessionUpdate = combine( set("date", new Date()), set("junk", new String("Now there is one")));
            UpdateResult res =  db.getCollection(m_col).updateOne(sessionFilter,
                    sessionUpdate,
                    new UpdateOptions().upsert(true));

            logger.debug("Init JsonDocument document: " +
                    db.getCollection(m_col).find(sessionFilter).first().toJson(prettyPrint));

        } catch (Exception e) {
            logger.error("Exception during session init: ");
            e.printStackTrace();
        }
    }

    /**
     * Change data capture scenario
     * Work on resumeAfter() and token persisting solution
     */
    public void runCDCReporters(){
        
        // Pool Executor
        ExecutorService poolExecutor = Executors.newFixedThreadPool(1);
        ArrayList<jClientGeneric> workforce = new ArrayList<>();
        workforce.add(new jClientWatcher( m_mongoClient, m_db, m_col));
        for (jClientGeneric w : workforce) {
            poolExecutor.execute(w);
        }
        poolExecutor.shutdown();
        try {
            poolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            logger.info("All Threads Complete: ... ");
        } catch (InterruptedException e) {
            logger.error("Await termination exception :" +  e.getMessage());
        } 
    }

    /**
     * CollStats & rs.status() example - single shoot
     */
    public void runReporters(){

        ExecutorService poolExecutor = Executors.newFixedThreadPool(m_threadCount);
        ArrayList<jClientGeneric> workforce = new ArrayList<jClientGeneric>();

        workforce.add(new jClientReporter( m_mongoClient, m_db, m_col));
        workforce.add(new jClientRSreporter( m_mongoClient, m_db, m_col));

        for (jClientGeneric w : workforce) {
            poolExecutor.execute(w);
        }
        poolExecutor.shutdown();
        try {
            poolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            logger.info("All Threads Complete: ... ");
        } catch (InterruptedException e) {
            logger.error("Await termination exception :" +  e.getMessage());
        }
    }

    /**
     * Periodic monitor for collStats and rs.Status()
     */
    public void runScheduledReporters(){

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(m_threadCount);
        jClientReporter task1 = new jClientReporter(m_mongoClient, m_db, m_col);
        executor.scheduleAtFixedRate(task1, 2, 2, TimeUnit.SECONDS);

        jClientRSreporter task2 = new jClientRSreporter(m_mongoClient, m_db, m_col);
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(task2, 2, 2, TimeUnit.SECONDS);

        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            logger.info("All Threads Complete: ... ");
        } catch (InterruptedException e) {
            logger.error("Await termination exception :" +  e.getMessage());
        }
    }

    /**
     * Periodic collStats
     */
    public void runReporter(){

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        jClientReporter reporter = new jClientReporter(m_mongoClient, m_db, m_col);
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(reporter, 2, 2, TimeUnit.SECONDS);
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            logger.info("All Threads Complete: ... ");
        } catch (InterruptedException e) {
            logger.error("Await termination exception :" +  e.getMessage());
        }
    }

    /**
     * Periodic rs.status()
     */
    public void runRSReporter(){

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        jClientRSreporter reporter = new jClientRSreporter(m_mongoClient, m_db, m_col);
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(reporter, 2, 2, TimeUnit.SECONDS);

        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            logger.info("All Threads Complete: ... ");
        } catch (InterruptedException e) {
            logger.error("Await termination exception :" +  e.getMessage());
        }
    }

    protected void finalize () throws Throwable  {
        logger.info( "jClient Finalize : -- ");
        if (m_mongoClient != null){
            logger.info( "jClient close connector : -- ");
            // m_mongoClient.close();
            m_mongoClient = null;
            if (m_mongoClient == null)
                logger.info( "jClient connector is not referenced.");
        }
//        super.finalize( );  
    }       
        
}
