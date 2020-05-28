package com.gpi;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.json.JsonWriterSettings;
import uk.dioxic.mgenerate.core.codec.MgenDocumentCodec;

import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * jClient
 * Main class launcher for the worker pools
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

    jClient(String conStr, String dbStr, String colStr, Boolean useCR, Boolean cleanup, int period){

        logger.info( "jClient connection string: " + conStr );
        m_db = dbStr;
        m_col = colStr;
        T = (long) period;
        try {
            MongoClientSettings clientSettings;
            if (useCR) {
                clientSettings = MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(conStr))
                        .codecRegistry(MgenDocumentCodec.getCodecRegistry())
                        .build();
            }
            else {

                clientSettings = MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(conStr))
                        .build();
             }
            m_mongoClient = MongoClients.create(clientSettings);
            initSession(cleanup);
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
    public void initSession(boolean cleanup) {
        try {
            if (cleanup) {
                m_mongoClient.getDatabase(m_db).getCollection(m_col).drop();
            }
            // If empty collection then create one (collstats exception)
            if (! m_mongoClient.getDatabase(m_db).listCollectionNames()
                    .into(new ArrayList<String>()).contains(m_col)) {
                m_mongoClient.getDatabase(m_db).createCollection(m_col);
            }
        } catch (Exception e) {
            logger.error("Exception during session init: ");
            e.printStackTrace();
        }
    }

    /**
     * Change data capture scenario
     * Work on resumeAfter() and token persisting solution
     * // @param rangeId Set entry range intervall for multiple range watcher (based on _id of the DocumentKey)
     */
    public void runCDCReporters(){
        
        // Pool Executor
        ExecutorService poolExecutor = Executors.newFixedThreadPool(2);
        ArrayList<jClientGeneric> workforce = new ArrayList<>();
        workforce.add(new jClientWatcher( m_mongoClient, m_db, m_col, 0));
        workforce.add(new jClientWatcher( m_mongoClient, m_db, m_col, 1000));
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
     * mgenerate example - single shoot
     */
    public void runWorkerOnce(String jsonModel){

        ExecutorService poolExecutor = Executors.newFixedThreadPool(1);
        ArrayList<jClientGeneric> workforce = new ArrayList<jClientGeneric>();

        jClientWorker wkr = new jClientWorker( m_mongoClient, m_db, m_col,jsonModel);
        if (wkr.get_isUp()) {
            workforce.add(wkr);
        }
        else return;

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
     * Case 3
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
     * Cas 1
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
     * Case 2
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
