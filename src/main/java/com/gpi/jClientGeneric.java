package com.gpi;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

import java.util.Date;

public class jClientGeneric implements Runnable{

    public MongoClient m_mongoCli;
    public String m_db, m_col;
    private static volatile WriteConcern wc;

    jClientGeneric(MongoClient mc, String dbName, String colName) {
        m_mongoCli = mc;
        m_db = dbName;
        m_col = colName;

        wc = WriteConcern.MAJORITY;
    }

    @Override
    public void run() {

        try {
            System.out.println("Generic reporter is doing a task during - Time - " + new Date());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized WriteConcern getWc() {
        return wc;
    }

    public void setWc(WriteConcern newWC) {
        synchronized (this) {
            this.wc = newWC;
        }
    }
}
