package org.noise_planet.noisemodelling.work


import groovy.sql.Sql
import org.apache.commons.cli.*
import org.h2.Driver
import org.h2gis.functions.factory.H2GISFunctions
import org.noise_planet.noisemodelling.wps.Database_Manager.Clean_Database
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_File
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import java.sql.ResultSet

class RunMatsim {

    static boolean postgis = false;
    static String postgis_db = "postgis_db_nm"
    static String postgis_user = "postgis_user"
    static String postgis_password = "postgis"

    static boolean doCleanDB = false;
    static boolean doImportOSMPbf = false;
    static boolean doExportRoads = false;
    static boolean doExportBuildings = false;
    static boolean doImportData = false;
    static boolean doExportResults = false;
    static boolean doTrafficSimulation = false;

    // all flags inside doSimulation
    static boolean doImportMatsimTraffic = true;
    static boolean doCreateReceiversFromMatsim = true;
    static boolean doCalculateNoisePropagation = true;
    static boolean doCalculateNoiseMap = true;
    static boolean doCalculateExposure = true;

    static int timeBinSize = 900
    static String receiversMethod = "closest"  // random, closest
    static String ignoreAgents = ""

    // acoustic propagation parameters
    static boolean diffHorizontal = true
    static boolean diffVertical = false
    static int reflOrder = 1
    static int maxReflDist = 50
    static def maxSrcDist = 750

    public static void main(String[] args) {
//        runNantesEdgt20p()
//        runNantesEdgt100p()
//        runNantesEdgt1p()
//        runLyonL63VEdgt20p()
        runChampignyEntd100p()
        // cli(args)
    }

    public static void runNantesEdgt20p() {

        String dbName = "file:///D:/SYMEXPO/matsim-nantes/edgt_20p/nantes_aire_urbaine/noisemodelling/noisemodelling"
        String osmFile = "D:\\SYMEXPO\\osm_maps\\nantes_aire_urbaine.osm.pbf";
        String inputsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_aire_urbaine\\noisemodelling\\inputs\\"
        String resultsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_aire_urbaine\\noisemodelling\\results\\"
        String matsimFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_aire_urbaine\\simulation_output\\"
        String srid = 2154;
        double populationFactor = 0.20;

        doCleanDB = false;
        doImportOSMPbf = false;

        doExportRoads = false;
        doExportBuildings = false;

        doTrafficSimulation = true;
        doExportResults = false;

        // all flags inside doSimulation
        doImportMatsimTraffic = false;
        doCreateReceiversFromMatsim = false;
        doCalculateNoisePropagation = false;
        doCalculateNoiseMap = false;
        doCalculateExposure = true;

        run(dbName, osmFile, matsimFolder, inputsFolder, resultsFolder, srid, populationFactor)
    }

    public static void runNantesEdgt100p() {

        String dbName = "file:///D:/SYMEXPO/matsim-nantes/edgt_100p/nantes_aire_urbaine/noisemodelling/noisemodelling"
        String osmFile = "D:\\SYMEXPO\\osm_maps\\nantes_aire_urbaine.osm.pbf";
        String inputsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_100p\\nantes_aire_urbaine\\noisemodelling\\inputs\\"
        String resultsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_100p\\nantes_aire_urbaine\\noisemodelling\\results\\"
        String matsimFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_100p\\nantes_aire_urbaine\\simulation_output\\"
        String srid = 2154;
        double populationFactor = 1.0;

        doCleanDB = false;
        doImportOSMPbf = false;

        doExportRoads = true;
        doExportBuildings = true;

        doTrafficSimulation = false;
        doExportResults = false;

        // all flags inside doSimulation
        doImportMatsimTraffic = false;
        doCreateReceiversFromMatsim = false;
        doCalculateNoisePropagation = false;
        doCalculateNoiseMap = false;
        doCalculateExposure = false;

        run(dbName, osmFile, matsimFolder, inputsFolder, resultsFolder, srid, populationFactor)
    }

    public static void runNantesEdgt1p() {

        String dbName = "file:///D:/SYMEXPO/matsim-nantes/edgt_1p/nantes_commune/noisemodelling/noisemodelling"
        String osmFile = "D:\\SYMEXPO\\osm_maps\\nantes_commune.osm.pbf";
        String inputsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_1p\\nantes_commune\\noisemodelling\\inputs\\"
        String resultsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_1p\\nantes_commune\\noisemodelling\\results\\"
        String matsimFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_1p\\nantes_commune\\simulation_output\\"
        String srid = 2154;
        double populationFactor = 0.01;

        doCleanDB = false;
        doImportOSMPbf = false;

        doExportRoads = true;
        doExportBuildings = false;

        doTrafficSimulation = true;
        doExportResults = false;

        // all flags inside doSimulation
        doImportMatsimTraffic = false;
        doCreateReceiversFromMatsim = true;
        doCalculateNoisePropagation = true;
        doCalculateNoiseMap = true;
        doCalculateExposure = false;

        run(dbName, osmFile, matsimFolder, inputsFolder, resultsFolder, srid, populationFactor)
    }

    public static void runLyonL63VEdgt20p() {

        String dbName = "file:///D:/SYMEXPO/matsim-lyon/edgt_20p/L63V/noisemodelling/noisemodelling"
        String osmFile = "D:\\SYMEXPO\\osm_maps\\L63V.osm.pbf";
        String inputsFolder = "D:\\SYMEXPO\\matsim-lyon\\edgt_20p\\L63V\\noisemodelling\\inputs\\"
        String resultsFolder = "D:\\SYMEXPO\\matsim-lyon\\edgt_20p\\L63V\\noisemodelling\\results\\"
        String matsimFolder = "D:\\SYMEXPO\\matsim-lyon\\edgt_20p\\L63V\\simulation_output\\"
        String srid = 2154;
        double populationFactor = 0.20;

        doCleanDB = false;
        doImportOSMPbf = false;

        doExportRoads = true;
        doExportBuildings = true;

        doTrafficSimulation = false;
        doExportResults = true;

        // all flags inside doSimulation
        doImportMatsimTraffic = true;
        doCreateReceiversFromMatsim = true;
        doCalculateNoisePropagation = true;
        doCalculateNoiseMap = true;
        doCalculateExposure = true;

        run(dbName, osmFile, matsimFolder, inputsFolder, resultsFolder, srid, populationFactor)
    }

    public static void runChampignyEntd100p() {

        String dbName = "file:///D:/SYMEXPO/matsim-94/entd_100p/champigny/noisemodelling/noisemodelling"
        String osmFile = "D:\\SYMEXPO\\osm_maps\\champigny.osm.pbf";
        String inputsFolder = "D:\\SYMEXPO\\matsim-94\\entd_100p\\champigny\\noisemodelling\\inputs\\"
        String resultsFolder = "D:\\SYMEXPO\\matsim-94\\entd_100p\\champigny\\noisemodelling\\results\\"
        String matsimFolder = "D:\\SYMEXPO\\matsim-94\\entd_100p\\champigny\\simulation_output"
        String srid = 2154;
        double populationFactor = 1.0;

        doCleanDB = false;
        doImportOSMPbf = false;

        doExportRoads = false;
        doExportBuildings = false;

        doTrafficSimulation = false;
        doExportResults = false;

        // all flags inside doSimulation
        doImportMatsimTraffic = true;
        doCreateReceiversFromMatsim = true;
        doCalculateNoisePropagation = true;
        doCalculateNoiseMap = true;
        doCalculateExposure = true;

        run(dbName, osmFile, matsimFolder, inputsFolder, resultsFolder, srid, populationFactor)
    }

    public static void cli(String[] args) {

        Options options = new Options();
        Properties configFile = new Properties();

        options.addRequiredOption("conf", "configFile", true, "[REQUIRED] Config file");

        options.addOption("clean", "cleanDB",false, "Clean the database");
        options.addOption("osm", "importOsmPbf", false, "Import OSM PBF file");
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
        String matsimFolder = configFile.get("MATSIM_DIR").toString();
        String inputsFolder = configFile.get("INPUTS_DIR").toString();
        String resultsFolder = configFile.get("RESULTS_DIR").toString();
        String srid = configFile.get("SRID").toString();
        double populationFactor = 1.0
        try {
            populationFactor = Double.parseDouble(configFile.get("POPULATION_FACTOR").toString());
        } catch (Exception ignored) {}

        try {
            new File(resultsFolder).mkdirs()
        }
        catch (Exception e) {
            System.out.println("Error trying to create the results folder : " + e.getMessage());
            return
        }

        doCleanDB = cmd.hasOption("cleanDB") || cmd.hasOption("doAll");
        doImportOSMPbf = cmd.hasOption("importOsmPbf") || cmd.hasOption("doAll");

        doExportRoads = cmd.hasOption("exportRoads") || cmd.hasOption("doAll");
        doExportBuildings = cmd.hasOption("exportBuildings") || cmd.hasOption("doAll");

        doTrafficSimulation = cmd.hasOption("runSimulation") || cmd.hasOption("doAll");
        doExportResults = cmd.hasOption("exportResults") || cmd.hasOption("doAll");

        if (cmd.hasOption("help") || cmd.hasOption("h")) {
            formatter.printHelp("gradlew run --args=\"...\"", options);
            return;
        }
        run(dbName, osmFile, matsimFolder, inputsFolder, resultsFolder, srid, populationFactor)
    }

    public static void run(String dbName, String osmFile, String matsimFolder, String inputsFolder, String resultsFolder, String srid, double populationFactor) {

        Connection connection;

        if (postgis) {
            String url = "jdbc:postgresql://localhost/" + postgis_db;
            Properties props = new Properties();
            props.setProperty("user", postgis_user);
            props.setProperty("password", postgis_password);
            connection = DriverManager.getConnection(url, props);
        }
        else {
            File dbFile = new File(URI.create(dbName));
            String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";";
            Driver.load();
            connection = DriverManager.getConnection(databasePath, "", "");
            H2GISFunctions.load(connection);
        }

        Files.createDirectories(Paths.get(resultsFolder))

        if (doCleanDB) {
            new Clean_Database().exec(connection, [
                    "areYouSure": "yes"
            ])
        }
        if (doImportOSMPbf) {
            new Import_OSM().exec(connection, [
                    "pathFile"        : osmFile,
                    "targetSRID"      : srid,
                    "ignoreGround": true,
                    "ignoreBuilding": false,
                    "ignoreRoads": true,
                    "removeTunnels": false
            ]);
        }

        if (doImportData) {
            println "Nothing to import"
        }

        if (doExportRoads) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "MATSIM_ROADS",
                    "exportPath"   : Paths.get(resultsFolder, "MATSIM_ROADS.geojson")
            ])
        }
        if (doExportBuildings) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "BUILDINGS",
                    "exportPath"   : Paths.get(resultsFolder, "BUILDINGS.geojson")
            ])
        }

        if (doTrafficSimulation) {

            if (doImportMatsimTraffic) {
                ImportMatsimTraffic.importMatsimTraffic(connection, [
                        "folder"           : matsimFolder,
                        "outTableName"     : "MATSIM_ROADS",
                        "link2GeometryFile": Paths.get(matsimFolder, "detailed_network.csv"), // absolute path
                        "timeBinSize"      : timeBinSize,
                        "skipUnused"       : "true",
                        "exportTraffic"    : "true",
                        "SRID"             : srid,
                        "ignoreAgents"     : ignoreAgents,
                        "perVehicleLevel"  : true,
                        "populationFactor" : populationFactor
                ]);
            }
            if (doCreateReceiversFromMatsim) {
                CreateReceiversOnBuildings.createReceiversOnBuildings(connection, [
                        "delta"            : 5.0,
                        "tableBuilding": "BUILDINGS",
                        "receiversTableName": "RECEIVERS",
                        "height": 4.0,
                        "fenceTableName" : postgis ? null : "BUILDINGS"
                ])
                ImportActivitesFromMatsim.importActivitesFromMatsim(connection, [
                        "facilitiesPath": Paths.get(matsimFolder, "output_facilities.xml.gz"),
                        "SRID"          : srid,
                        "outTableName"  : "ACTIVITIES"
                ]);
                if (receiversMethod == "random") {
                    AssignRandomReceiversToActivities.activitiesRandomReceiver(connection, [
                            "activitiesTable": "ACTIVITIES",
                            "buildingsTable" : "BUILDINGS",
                            "receiversTable" : "RECEIVERS",
                            "outTableName"   : "ACTIVITIES_RECEIVERS"
                    ]);
                } else {
                    AssignClosestReceiversToActivities.activitiesClosestReceiver(connection, [
                            "activitiesTable": "ACTIVITIES",
                            "receiversTable" : "RECEIVERS",
                            "outTableName"   : "ACTIVITIES_RECEIVERS"
                    ]);
                }
            }

            if (doCalculateNoisePropagation) {
                Create0dBSourceFromRoads.create0dBSourceFromRoads(connection, [
                        "roadsTableName"  : "MATSIM_ROADS",
                        "sourcesTableName": "SOURCES_0DB"
                ]);
                CalculateNoiseMapFromSource.calculateNoiseMap(connection, [
                        "tableBuilding"     : "BUILDINGS",
                        "tableReceivers"    : "ACTIVITIES_RECEIVERS",
                        "tableSources"      : "SOURCES_0DB",
                        "confMaxSrcDist"    : maxSrcDist,
                        "confMaxReflDist"   : maxReflDist,
                        "confReflOrder"     : reflOrder,
                        "confSkipLevening"  : true,
                        "confSkipLnight"    : true,
                        "confSkipLden"      : true,
                        "confThreadNumber"  : 16,
                        "confExportSourceId": true,
                        "confDiffVertical"  : diffVertical,
                        "confDiffHorizontal": diffHorizontal
                ]);
                new Sql(connection).execute("ALTER TABLE LDAY_GEOM RENAME TO ATTENUATION_TRAFFIC")
            }
            if (doCalculateNoiseMap) {
                CalculateNoiseMapFromAttenuation.calculateNoiseMap(connection, [
                        "matsimRoads"     : "MATSIM_ROADS",
                        "matsimRoadsLw"   : "MATSIM_ROADS_LW",
                        "attenuationTable": "ATTENUATION_TRAFFIC",
                        "receiversTable"  : "ACTIVITIES_RECEIVERS",
                        "outTableName"    : "RESULT_GEOM",
                        "timeBinSize"     : timeBinSize
                ])
            }

            if (doCalculateExposure) {
                CalculateMatsimAgentExposure.calculateMatsimAgentExposure(connection, [
                        "experiencedPlansFile"  : Paths.get(matsimFolder, "output_experienced_plans.xml.gz"),
                        "plansFile"             : Paths.get(matsimFolder, "output_plans.xml.gz"),
                        "personsCsvFile"        : Paths.get(matsimFolder, "output_persons.csv.gz"),
                        "SRID"                  : srid,
                        "receiversTable"        : "ACTIVITIES_RECEIVERS",
                        "outTableName"          : "EXPOSURES",
                        "dataTable"             : "RESULT_GEOM",
                        "timeBinSize"           : timeBinSize,
                ])
            }
        }
        if (doExportResults) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "RESULT_GEOM",
                    "exportPath"   : Paths.get(resultsFolder, "RESULT_GEOM.shp")
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
