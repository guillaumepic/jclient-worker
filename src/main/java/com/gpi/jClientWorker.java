package com.gpi;

import com.mongodb.MongoClient;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import com.mongodb.client.model.Sorts;
import java.util.Arrays;
import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.CommandResult;
import org.bson.Document;
import org.bson.types.Binary;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.util.*;

import de.svenjacobs.loremipsum.LoremIpsum;

public class jClientWorker implements Runnable {

    private int m_id;
    private MongoClient m_mongoCli;
    private String m_db, m_col;
    Document m_internalDoc;
    private Random m_rng;
    private String m_loremText = null;
    private static volatile WriteConcern wc;
    // private static Binary m_blobData = null;

    jClientWorker(MongoClient mc, String dbName, String colName, int id) {
        m_mongoCli = mc;
        m_db = dbName;
        m_col = colName;
        m_id = id;
        m_rng = new Random();

        wc= WriteConcern.MAJORITY;
    }

    Block<Document> printBlock = new Block<Document>() {
        @Override
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };

    private String CreateString(int length) {

        if (m_loremText == null) {
            m_loremText = "";
            LoremIpsum loremIpsum = new LoremIpsum();
            // System.out.println("Generating sample data");
            m_loremText = loremIpsum.getWords(1000);
        }

        StringBuilder sb = new StringBuilder();
        Double d = m_rng.nextDouble();

        int loremLen = 512;
        int r = (int) Math.abs(Math.floor(d * (m_loremText.length() - (loremLen + 20))));
        int e = r + loremLen;

        while (m_loremText.charAt(r) != ' ')
            r++;
        r++;
        while (m_loremText.charAt(e) != ' ')
            e++;
        String chunk = m_loremText.substring(r, e);

        sb.append(chunk);

        // Double to size
        while (sb.length() < length) {
            // System.out.println(" SB " + sb.length() + " of " + length);
            sb.append(sb.toString());
        }

        // Trim to fit
        String rs = sb.toString().substring(0, length);

        // Remove partial words
        r = 0;
        e = rs.length() - 1;
        while (rs.charAt(e) != ' ')
            e--;
        rs = rs.substring(r, e);
        return rs;
    }

    private void doSomething() {

        MongoDatabase db = m_mongoCli.getDatabase(m_db);
        String p_text = CreateString(256);
        try {
            Document item = new Document().append("date", new Date()).append("writing", p_text).append("thread", m_id);
            db.getCollection(m_col).withWriteConcern(wc).insertOne(item);
        } catch (Exception e) {
            System.out.print("Exception during Insert error : ");
            e.printStackTrace();
        };

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        System.out.println("jClient :: " + Integer.toString(m_id) + ": -- something done at " + dateFormat.format(date));

        try {
            MongoCollection<Document> p_col = db.getCollection(m_col).withReadPreference(ReadPreference.primary())
                    .withReadConcern(ReadConcern.MAJORITY).withWriteConcern(WriteConcern.MAJORITY);
            p_col.find(eq("thread", m_id)).sort(Sorts.ascending("date"))
                    .projection(fields(include("thread", "date"), excludeId())).limit(2).forEach(printBlock);
        } catch (Exception e) {
            System.out.print("Exception during Insert error : ");
            e.printStackTrace();
        };
    }

    public void run() {
        try {
            doSomething();
        } catch (Exception e) {
            System.out.println("Error jClientWorker: " + e.getMessage());
        }
    }
}