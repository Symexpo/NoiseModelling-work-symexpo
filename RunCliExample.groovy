package org.noise_planet.noisemodelling.work


import groovy.sql.Sql
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.MissingOptionException
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.h2gis.functions.factory.H2GISDBFactory
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.noisemodelling.wps.Database_Manager.Clean_Database
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM_Pbf

import java.nio.file.Paths
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet

class RunCliExample {

    static boolean doCleanDB = false;
    static boolean doImportOSMPbf = false;
    static boolean doExportRoads = false;
    static boolean doExportBuildings = false;
    static boolean doImportData = false;
    static boolean doExportResults = false;
    static boolean doSimulation = false;

    public static void main(String[] args) {
        cli(args)
    }

    public static void cli(String[] args) {

        Options options = new Options();
        Properties configFile = new Properties();

        options.addRequiredOption("conf", "configFile", true, "[REQUIRED] Config file");

        options.addOption("clean", "cleanDB",false, "Clean the database");
        options.addOption("osm", "importOsmPbf", false, "Import OSM PBF file");
        options.addOption("import", "importData", false, "Import data");
        options.addOption("roads", "exportRoads", false, "Export roads");
        options.addOption("buildings", "exportBuildings", false, "Export buildings");
        options.addOption("run", "runSimulation", false, "Run simulation");
        options.addOption("results", "exportResults", false, "Export results");
        options.addOption("all", "doAll", false, "Activate all flags (cleans up database and run everything)");
        options.addOption("h", "help", false, "Display help");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd
        HelpFormatter formatter = new HelpFormatter();
        try {
            cmd = parser.parse(options, args);
        }
        catch (MissingOptionException e) {
            println ("Missing option: " + e.getMessage());
            formatter.printHelp("gradlew run --args=\"...\"", options);
            return;
        }
        catch (ParseException e) {
            println ("CLI options error: " + e.getMessage());
            formatter.printHelp("gradlew run --args=\"...\"", options);
            return;
        }

        try {
            File file = new File(cmd.getOptionValue("configFile"));
            configFile.load(new FileInputStream(file));
        }
        catch (Exception e) {
            System.out.println("Config file not found : " + e.getMessage());
            return
        }

        String dbName = configFile.get("DB_NAME").toString();
        try {
            File file = new File(dbName);
            dbName = file.toPath().toAbsolutePath().toUri().toString();
        }
        catch (Exception e) {
            System.out.println("Database file path not reachable : " + e.getMessage());
            return
        }
        String osmFile = configFile.get("OSM_FILE_PATH").toString();
        String inputFolder = configFile.get("INPUTS_DIR").toString();
        String resultsFolder = configFile.get("RESULTS_DIR").toString();
        String srid = configFile.get("SRID").toString();

        try {
            new File(resultsFolder).mkdirs()
        }
        catch (Exception e) {
            System.out.println("Error trying to create the results folder : " + e.getMessage());
            return
        }

        doCleanDB = cmd.hasOption("cleanDB") || cmd.hasOption("doAll");
        doImportOSMPbf = cmd.hasOption("importOsmPbf") || cmd.hasOption("doAll");
        doImportData = cmd.hasOption("importData") || cmd.hasOption("doAll");

        doExportRoads = cmd.hasOption("exportRoads") || cmd.hasOption("doAll");
        doExportBuildings = cmd.hasOption("exportBuildings") || cmd.hasOption("doAll");

        doSimulation = cmd.hasOption("runSimulation") || cmd.hasOption("doAll");
        doExportResults = cmd.hasOption("exportResults") || cmd.hasOption("doAll");

        if (cmd.hasOption("help") || cmd.hasOption("h")) {
            formatter.printHelp("gradlew run --args=\"...\"", options);
            return;
        }
        run(dbName, osmFile, inputFolder, resultsFolder, srid)
    }

    public static void run(String dbName, String osmFile, String inputFolder, String resultsFolder, String srid) {

        Connection connection;

        File dbFile = new File(URI.create(dbName+".mv.db"));
        if (dbFile.exists()) {
            connection = JDBCUtilities.wrapConnection(H2GISDBFactory.openSpatialDataBase(dbName));
        }
        else {
            connection = JDBCUtilities.wrapConnection(H2GISDBFactory.createSpatialDataBase(dbName, true));
        }

        Sql sql = new Sql(connection)

        if (doCleanDB) {
            new Clean_Database().exec(connection, [
                    "areYouSure": "yes"
            ])
        }
        if (doImportOSMPbf) {
            new Import_OSM_Pbf().exec(connection, [
                    "pathFile"        : osmFile,
                    "targetSRID"      : srid,
                    "ignoreGround": true,
                    "ignoreBuilding": false,
                    "ignoreRoads": false,
                    "removeTunnels": false,
            ]);
        }
        if (doImportData) {
            println "Nothing to import"
        }

        if (doExportRoads) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "ROADS",
                    "exportPath"   : Paths.get(resultsFolder, "ROADS.shp")
            ])
        }
        if (doExportBuildings) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "BUILDINGS",
                    "exportPath"   : Paths.get(resultsFolder, "BUILDINGS.shp")
            ])
        }

        if (doSimulation) {

            if (!tableExists(connection, "ROADS")) {
                System.err.println("table ROADS not found")
                return
            }
            if (!tableExists(connection, "BUILDINGS")) {
                System.err.println("table BUILDINGS not found")
                return
            }
            if (!tableExists(connection, "RECEIVERS")) {
                System.err.println("table RECEIVERS not found")
                return
            }

            // TODO : Run sim
        }

        if (doExportResults) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "RESULT_LEVELS",
                    "exportPath"   : Paths.get(resultsFolder, "RESULT_LEVELS.shp")
            ])
        }

        connection.close()
    }

    static boolean tableExists(Connection connection, String table) {
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet rs = dbMeta.getTables(null, null, table, null);
        boolean table_found = false;
        if (rs.next()) {
            table_found = true
        }
        return table_found
    }

    static boolean columnExists(Connection connection, String table, String column_name) {
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet rs = dbMeta.getColumns(null, null, table, column_name);
        boolean col_found = false;
        if (rs.next()) {
            col_found = true
        }
        return col_found
    }

    static boolean indexExists(Connection connection, String table, String column_name) {
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet rs = dbMeta.getIndexInfo(null, null, table, false, false);
        boolean index_found = false;
        while (rs.next()) {
            String column = rs.getString("COLUMN_NAME");
            String pos = rs.getString("ORDINAL_POSITION");
            if (column == column_name && pos == "1") {
                index_found = true;
            }
        }
        return index_found
    }

    static void ensureIndex(Connection connection, String table, String column_name, boolean spatial) {
        if (!indexExists(connection, table, column_name)) {
            Sql sql = new Sql(connection)
            sql.execute("CREATE " + (spatial ? "SPATIAL " : "") + "INDEX ON " + table + " (" + column_name + ")");
        }
    }

}
