package com.gpi;

import org.apache.commons.cli.*;

import java.util.Locale;

/**
 * Dummy app utility to test various worker pool scenario
 * monitor rs.status(), collstats
 * Change streams
 * ...
 */

public class App {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(App.class);

    public static void main(String[] args) throws InterruptedException {

        // Set English/US for compliance with Faker
        // Locale lang = Locale.getDefault(); // fr_FR
        Locale.setDefault(new Locale("en", "US"));

        Options options = new Options();

        Option uri = new Option("u", "uri", true, "Mongo connection string ie, mongodb://localhost:27017");
        uri.setRequired(true);
        options.addOption(uri);

        Option usecase = new Option("uc", "usecase", true, "Use case: 1 - single collection stats 2 - RS status\n" +
                "Use case 2 - periodic rs.status()\n" +
                "Use case 3 - periodic rs.status() & runCommand({collstats:mycol})\n" +
                "Use case 4 - periodic rs.status() & runCommand({collstats:mycol})\n" +
                "Use case 5 - run dual watchers and manage pending status \n" +
                "Use case 6 - run (once) an insertMany generating documents against json template");
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

        Option cleanup = new Option("cu", "cleanup", true, "Option to drop working collection. Default is false");
        cleanup.setRequired(false);
        options.addOption(cleanup);

        Option jsonModel = new Option("m", "model", true, "Optional file name for template json model used by mgenerate. Default is null");
        jsonModel.setRequired(false);
        options.addOption(jsonModel);

        Option mode = new Option("p", "periodic", true, "Mode single or peridic");
        mode.setRequired(false);
        options.addOption(mode);

        String header = "Option arguments may be in any order";
        String footer = "This is work in progress around MongoDB client workers.\n";
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("jClient", header, options, footer, true);

        String connectionURI = null;
        int uc = 0;
        String db = "jdb", col = "jcol";
        boolean periodic = false, clean = false;
        boolean go = true;
        String model = null;
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
                logger.info("Option uc is present.  The use case is: " + commandLine.getOptionValue("uc"));
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

            if (commandLine.hasOption("cu")) {
                clean = Boolean.parseBoolean(commandLine.getOptionValue("cu"));
                logger.info("option cleanup is: " + clean);
            }

            if (commandLine.hasOption("m")) {
                model = commandLine.getOptionValue("m");
                logger.info("option json model template is: " + model);
            }

            if (commandLine.hasOption("p")) {
                Period = Integer.parseInt(commandLine.getOptionValue("p"));
                logger.info("Option Period (unused) is : " + Period);
            }

        } catch (ParseException exception) {
            logger.error("Parse error: " + exception.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(0);
        }

        // Missing args
        if (!go) {
            formatter.printHelp("utility-name", options);
            System.exit(0);
        }

        // Main switch case
        jClient o_jClient;
        switch (uc) {
            case 1:
                logger.info("Build single jclient reporter ie, doing db.runCommand({collstats:1}), periodic 2s");
                o_jClient = new jClient(connectionURI, db, col, false, clean, Period);
                o_jClient.runReporter();
            case 2:
                logger.info("Build single jclient RS reporter ie, periodic task running rs.status()");
                o_jClient = new jClient(connectionURI, db, col, false, clean, Period);
                o_jClient.runRSReporter();
            case 3:
                logger.info("Build jclient reporters status/collstats in parallel. (note : is a service executor loop) ");
                o_jClient = new jClient(connectionURI, db, col, false, clean, Period);
                o_jClient.runReporters();
            case 4:
                logger.info("Build jclient reporters status/collstats in parallel. Periodic");
                o_jClient = new jClient(connectionURI, db, col, false, clean, Period);
                o_jClient.runScheduledReporters();
            case 5:
                logger.info("Build scheduled jclient watcher ie, open change stream with token mgt");
                o_jClient = new jClient(connectionURI, db, col, false, clean, Period);
                o_jClient.runCDCReporters();
            case 6:
                logger.info("Build one shot jclient worker: Mgenerate CodecRegistry");
                if (model == null) {
                    logger.error("Missing template json model to generate ... Terminating");
                    formatter.printHelp("utility-name", options);
                    return;
                }
                o_jClient = new jClient(connectionURI, db, col, true, true, Period);
                o_jClient.runWorkerOnce(model);
            default:
                logger.info("Ciao jClient !! \n");
        }

        System.runFinalizersOnExit(true);
    }
}
