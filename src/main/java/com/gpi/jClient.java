package com.gpi;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.Server;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

/**
 * jClient
 *
 */
public class jClient
{

    private MongoClient m_mongoClient;
    private String m_db, m_col;
    static long start, end;
    static int m_threadCount = 2;
    static long T = 10;
    private static int count=0;

    JsonWriterSettings prettyPrint = JsonWriterSettings.builder().indent(true).build();

    jClient(String conStr, String dbStr, String colStr, int period){

        System.out.println( "jClient Constructor connection string: " + conStr );
        m_db = dbStr;
        m_col = colStr;
        T = (long) period;
        try {

/*            MongoClientOptions options = MongoClientOptions.builder()
                    .retryWrites(true)
                    .writeConcern(WriteConcern.MAJORITY)
                    .applicationName("jClient")
                    .requiredReplicaSetName("rsjunk")
                    .build();

            List<ServerAddress> ServerList = Arrays.asList(new ServerAddress("localhost", 27017),
                    new ServerAddress("localhost", 27018),
                    new ServerAddress("localhost", 27019),
                    new ServerAddress("localhost", 27020)
                    );

             m_mongoClient = new MongoClient(ServerList, options);*/

             // Alt connection string
             m_mongoClient = new MongoClient(new MongoClientURI(conStr));

        }
        catch(Exception e)
            {
                e.printStackTrace();
            }
    }

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

            System.out.println(db.getCollection(m_col).find(sessionFilter).first().toJson(prettyPrint));
            System.out.println("InitSession .... ");

        } catch (Exception e) {
            System.out.print("Exception during Update error : ");
            e.printStackTrace();
        }
    }

    public void runWorkers(){
        
        // Using a thread pool we keep filled
        ExecutorService poolExecutor = Executors.newFixedThreadPool(m_threadCount);
        int threadIdStart = 4346; // complete rand
        System.out.println("threadIdStart="+threadIdStart);
        ArrayList<jClientWorker> workforce = new ArrayList<jClientWorker>();
        for (int i = threadIdStart; i < (m_threadCount + threadIdStart); i++) {
            System.out.println("Creating worker " + i);
            workforce.add(new jClientWorker( m_mongoClient, m_db, m_col, i));    
        }
        for (jClientWorker w : workforce) {
            poolExecutor.execute(w);
        }
        poolExecutor.shutdown();
        try {
            poolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            System.out.println("All Threads Complete: ... ");
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } 
    }

    public void runReporters(){

        // Using a thread pool we keep filled
        ExecutorService poolExecutor = Executors.newFixedThreadPool(m_threadCount);
        int threadIdStart = 4346; // complete rand

        ArrayList<jClientGeneric> workforce = new ArrayList<jClientGeneric>();

        workforce.add(new jClientReporter( m_mongoClient, m_db, m_col));
        workforce.add(new jClientRSreporter( m_mongoClient, m_db, m_col));

        for (jClientGeneric w : workforce) {
            poolExecutor.execute(w);
        }
        poolExecutor.shutdown();
        try {
            poolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            System.out.println("All Threads Complete: ... ");
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    public void runScheduledReporters(){

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(m_threadCount);
        int threadIdStart = 4346; // complete rand

        jClientReporter task1 = new jClientReporter(m_mongoClient, m_db, m_col);
        executor.scheduleAtFixedRate(task1, 2, 2, TimeUnit.SECONDS);

        jClientRSreporter task2 = new jClientRSreporter(m_mongoClient, m_db, m_col);
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(task2, 2, 2, TimeUnit.SECONDS);

        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            System.out.println("All Threads Complete: ... ");
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    public void runReporter(){

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        jClientReporter reporter = new jClientReporter(m_mongoClient, m_db, m_col);
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(reporter, 2, 2, TimeUnit.SECONDS);
        System.out.println("executor is up ... ");
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            System.out.println("All Threads Complete: ... ");
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    public void runRSReporter(){

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        jClientRSreporter reporter = new jClientRSreporter(m_mongoClient, m_db, m_col);
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(reporter, 2, 2, TimeUnit.SECONDS);

        System.out.println("executor is up ... ");
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            System.out.println("All Threads Complete: ... ");
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }

    }

    protected void finalize () throws Throwable  {
        System.out.println( "jClient Finalize : -- ");
        if (m_mongoClient != null){
            System.out.println( "jClient close connector : -- ");
            // m_mongoClient.close();
            m_mongoClient = null;
            if (m_mongoClient == null)
                System.out.println( "jClient connector is not referenced.");
        }
//        super.finalize( );  
    }       
        
}
