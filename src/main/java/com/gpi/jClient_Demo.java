package com.gpi;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;

import org.apache.commons.cli.Options;
import org.bson.Document;
import com.mongodb.MongoClientOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * jClient
 *
 */
public class jClient_Demo
{
    private MongoClient m_mongoClient;
    private String m_db, m_col;
    static long start, end;
    static int m_threadCount = 5;
    
    jClient_Demo(String conStr, String dbStr, String colStr){
        System.out.println( "jClient Constructor : -- " + conStr );
        m_db = dbStr;
        m_col = colStr;      
        try {
            m_mongoClient = new MongoClient(new MongoClientURI(conStr));
        }
        catch(Exception e)
            {
                e.printStackTrace();
            }
    }

    public void do_findOneAndUpdate(){

        System.out.println( "jClient findOneAndUpdate : -- " + m_db);

        MongoDatabase db;
        db = m_mongoClient.getDatabase(m_db);
        FindOneAndUpdateOptions opt = new FindOneAndUpdateOptions();
        opt.projection(new Document().append("comments.name", 1));
        opt.returnDocument(ReturnDocument.AFTER); 
        opt.upsert(true);
        Document query= new Document();
        List<Integer> comment = new ArrayList();
        comment.add(1);
        query.append("_id",comment);
        Document update = new Document();
        update.append("gpi", "gilles");

        System.out.println( "jClient findOneAndUpdate : print args: \n" + query + " \n" + update  );

        db.getCollection(m_col).findOneAndUpdate(query, update, opt);

        System.out.println( "-------------- ");
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
