package com.gpi;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import uk.dioxic.mgenerate.core.Template;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class jClientWorker extends jClientGeneric implements Runnable {

    private static final Logger logger = Logger.getLogger(jClientReporter.class);
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSSZ");
    private MongoDatabase db;
    private String m_jsonModelName;
    private Boolean m_isUp=true;
    private int m_bulkSize=10;

    jClientWorker(MongoClient mc, String dbName, String colName, String jsonModelName) {
        super(mc, dbName, colName);
        db = m_mongoCli.getDatabase(m_db);
        m_jsonModelName = jsonModelName;
        if (getClass().getClassLoader().getResource(m_jsonModelName)==null)
        {
            logger.warn("JClientWorker doesn't have a model resource. json model not found");
            m_isUp=false;
        }
    }

    public Boolean get_isUp() {
        return m_isUp;
    }

    private void doInsertMany() {

        MongoCollection<Template> collection = m_mongoCli
                .getDatabase(m_db)
                .getCollection(m_col, Template.class);

        try {
            ClassLoader classLoader = getClass().getClassLoader();
            String JsonModelPrimary = classLoader.getResource(m_jsonModelName).getPath();
            // Template template;
            List<Template> documents = new ArrayList<>();
            for (int i=0; i<m_bulkSize; i++) {
                documents.add(Template.from(JsonModelPrimary));
                // template = Template.from(JsonModelPrimary);
                // collection.insertOne(template);
                /*
                Document doc = template.getDocument();
                if (doc.containsKey("jClientArray"))
                {
                    List<Document> outerItems = null;
                    logger.info("Good we are !");
                    Document x = doc.get("jClientArray", doc.getClass());
                }*/
            }
            collection.insertMany(documents);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            doInsertMany();
        } catch (Exception e) {
            System.out.println("Error jClientWorker: " + e.getMessage());
        }
    }
}