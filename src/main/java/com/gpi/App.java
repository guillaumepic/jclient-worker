package com.gpi;

import org.apache.commons.cli.*;

/**
 * Dummy app utility to test various worker pool scenario
 * monitor rs.status(), collstats
 * Change streams
 * ...
 */

public class App {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(App.class);

    public static void main(String[] args) throws InterruptedException  {

        Options options = new Options();

        Option uri = new Option("u", "uri", true, "Mongo connection string ie, mongodb://localhost:27017");
        uri.setRequired(true);
        options.addOption(uri);

        Option usecase = new Option("uc", "usecase", true, "Use case: 1 - single collection stats 2 - RS status");
        usecase.setRequired(false);
        options.addOption(usecase);

        Option ssl = new Option("s", "ssl", false, "Enable TLS/SSL host certification true/false");
        ssl.setRequired(false);
        options.addOption(ssl);

        Option database = new Option("d", "db", true, "Database name. Default is jdb");
        database.setRequired(false);
        options.addOption(database);

        Option collection = new Option("c", "col", true, "Collection name. Default is jcol");
        collection.setRequired(false);
        options.addOption(collection);

        Option mode = new Option("p", "periodic", true, "Mode single or peridic");
        mode.setRequired(false);
        options.addOption(mode);

        String header = "Options, flags and arguments may be in any order";
        String footer = "This is work in progress around MongoDB client workers.\n GPI March 2020\n";
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("jClient", header, options, footer, true);

        String connectionURI = null;
        int uc = 0;
        String db = "jdb", col = "jcol";
        boolean periodic = false;
        boolean go = true;
        int Period = 10;
        CommandLine commandLine;
        CommandLineParser parser = new DefaultParser();

        // Parsing Options
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption("u")) {
                connectionURI = commandLine.getOptionValue("u");
                logger.info("Option u is present.  The value is: " + connectionURI);
            } else {
                logger.warn("Option u is not present ... but required");
                go = false;
            }

            if (commandLine.hasOption("uc")) {
                uc = Integer.parseInt(commandLine.getOptionValue("uc"));
                logger.info("Option uc is present.  The use case is: " + commandLine.getOptionValue("uc") );
            } else {
                System.out.print("Option Scenario is not present");
                go = false;
            }

            if (commandLine.hasOption("d")) {
                db = commandLine.getOptionValue("d");
                logger.info("Option database name is: " + db);
            }

            if (commandLine.hasOption("c")) {
                col = commandLine.getOptionValue("c");
                logger.info("option collection name is: " + col);
            }

            if (commandLine.hasOption("p")) {
                Period = Integer.parseInt(commandLine.getOptionValue("p"));
                logger.info("Option Period (unused) is : " + Period);
            }

        } catch (ParseException exception) {
            logger.error("Parse error: "+ exception.getMessage() );
            formatter.printHelp("utility-name", options);
            System.exit(0);
        }

        // Missing args
        if (!go) {
            formatter.printHelp("utility-name", options);
            System.exit(0);
        }

        // Switch use case
        logger.info("Build jclient");
        jClient o_jClient = new jClient(connectionURI, db, col, Period);
        switch (uc) {
            case 1:
                logger.warn("Build single jclient reporter ie, doing db.runCommand({collstats:1})");
                o_jClient.runReporter();
            case 2:
                logger.info("Build single jclient RS reporter ie, periodic task running rs.status()");
                o_jClient.initSession();
                o_jClient.runRSReporter();
            case 3:
                logger.info("Build jclient reporters status/collstats in parallel. One shoot");
                o_jClient.initSession();
                o_jClient.runReporters();
            case 4:
                logger.info("Build jclient reporters status/collstats in parallel. Periodic");
                o_jClient.initSession();
                o_jClient.runScheduledReporters();
            case 5:
                System.out.print("Build scheduled jclient watcher ie, open change stream with token mgt");
                o_jClient.runCDCReporters();
            default:
                System.out.print("Ciao jClient !! \n");
        }

        System.runFinalizersOnExit(true);
    }
}
