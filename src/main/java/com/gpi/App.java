package com.gpi;

import org.apache.commons.cli.*;
import java.util.logging.Logger;

/**
 * Hello !
 */

public class App {

    Logger logger;
    //private static int count=0;

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

        String header = "              Options, flags and arguments may be in any order";
        String footer = "This is work in progress around MongoDB injection workload.\n GPI March 2020\n";
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("jClient", header, options, footer, true);

        String connectionURI = null;
        int uc = 0;
        String db = "jdb", col = "jcol";
        boolean periodic = false;
        int Period = 10;
        CommandLine commandLine;
        CommandLineParser parser = new DefaultParser();

        // Parsing Options
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption("u")) {
                System.out.print("Option u is present.  The value is: ");
                connectionURI = commandLine.getOptionValue("u");
                System.out.println(connectionURI + "\n");
            } else {
                System.out.print("Option u is not present\n");
                formatter.printHelp("utility-name", options);
                System.exit(1);
            }

            if (commandLine.hasOption("uc")) {
                System.out.print("Option uc is present.  The value is: ");
                uc = Integer.parseInt(commandLine.getOptionValue("uc"));
                System.out.println("Scenario is " + commandLine.getOptionValue("uc") +"\n");
            } else {
                System.out.print("Option Scenario is not present\n");
                formatter.printHelp("utility-name", options);
                System.exit(1);
            }

            if (commandLine.hasOption("d")) {
                System.out.print("Option d is present.  The value is: ");
                db = commandLine.getOptionValue("d");
                System.out.println(db + "\n");
            }

            if (commandLine.hasOption("c")) {
                System.out.print("Option c is present.  The value is: ");
                col = commandLine.getOptionValue("c");
                System.out.println(col + "\n");
            }

            if (commandLine.hasOption("p")) {
                System.out.print("Option p is present. Periodic mode ");
                periodic = true;
                Period = Integer.parseInt(commandLine.getOptionValue("p"));
            }

        } catch (ParseException exception) {
            System.out.print("Parse error: ");
            System.out.println(exception.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

        // Enable MongoDB logging in general
        System.setProperty("DEBUG.MONGO", "true");
        // Enable DB operation tracing
        System.setProperty("DB.TRACE", "true");

        //
        System.out.print("Build jclient  \n");
        jClient o_jClient = new jClient(connectionURI, db, col, Period);
        o_jClient.initSession();
        System.out.print("Build jclient initSession()  \n");
        switch (uc) {
            case 1:
                System.out.print("Build jclient reporter (collstats): \n");
                o_jClient.runReporter();
            case 2:
                System.out.print("Build jclient RS reporter (rsstatus): \n");
                o_jClient.runRSReporter();
            case 3:
                System.out.print("Build jclient reporter task force: \n");
                o_jClient.runReporters();
            case 4:
                System.out.print("Build scheduled jclient reporter task force: \n");
                o_jClient.runScheduledReporters();
            default:
                System.out.print("Ciao jClient !! \n");
        }

        //System.runFinalizersOnExit(true);
    }
}
