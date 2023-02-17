package org.noise_planet.noisemodelling.work

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.h2.Driver
import org.h2gis.functions.factory.H2GISFunctions
import org.locationtech.jts.geom.Geometry
import org.noise_planet.noisemodelling.wps.Acoustic_Tools.Create_Isosurface
import org.noise_planet.noisemodelling.wps.Database_Manager.Clean_Database
import org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_File
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM
import org.noise_planet.noisemodelling.wps.Receivers.Delaunay_Grid
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.*

class RunMatsimConstructionDelaunay {

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
    static boolean doChantierSimulation = false;

    // all flags inside doSimulation
    static boolean doImportMatsimTraffic = true;
    static boolean doCreateReceiversFromMatsim = false;
    static boolean doCreateReceiversGrid = true;
    static boolean doCalculateNoisePropagation = true;
    static boolean doCalculateContourPropagation = true;
    static boolean doCalculateNoiseMap = true;
    static boolean doCalculateContourMaps = true;
    static boolean doExportContoursMaps = true;
    static boolean doCalculateExposure = true;

    // all flags inside doChantierSimulation
    static boolean doChantierSimulationPropagation = true;
    static boolean doChantierSimulationEmission = true;
    static boolean doChantierSimulationNoiseMap = true;
    static boolean doChantierSimulationNoiseMapContours = true;
    static boolean doExportContoursMapsConstruction = true;

    static List<String> hourTimeStrings = ["0_1", "1_2", "2_3", "3_4", "4_5", "5_6", "6_7", "7_8", "8_9", "9_10", "10_11", "11_12", "12_13", "13_14", "14_15", "15_16", "16_17", "17_18", "18_19", "19_20", "20_21", "21_22", "22_23", "23_24"]
    static List<String> quarterHourTimeStrings = ["0h00_0h15", "0h15_0h30", "0h30_0h45", "0h45_1h00", "1h00_1h15", "1h15_1h30", "1h30_1h45", "1h45_2h00", "2h00_2h15", "2h15_2h30", "2h30_2h45", "2h45_3h00", "3h00_3h15", "3h15_3h30", "3h30_3h45", "3h45_4h00", "4h00_4h15", "4h15_4h30", "4h30_4h45", "4h45_5h00", "5h00_5h15", "5h15_5h30", "5h30_5h45", "5h45_6h00", "6h00_6h15", "6h15_6h30", "6h30_6h45", "6h45_7h00", "7h00_7h15", "7h15_7h30", "7h30_7h45", "7h45_8h00", "8h00_8h15", "8h15_8h30", "8h30_8h45", "8h45_9h00", "9h00_9h15", "9h15_9h30", "9h30_9h45", "9h45_10h00", "10h00_10h15", "10h15_10h30", "10h30_10h45", "10h45_11h00", "11h00_11h15", "11h15_11h30", "11h30_11h45", "11h45_12h00", "12h00_12h15", "12h15_12h30", "12h30_12h45", "12h45_13h00", "13h00_13h15", "13h15_13h30", "13h30_13h45", "13h45_14h00", "14h00_14h15", "14h15_14h30", "14h30_14h45", "14h45_15h00", "15h00_15h15", "15h15_15h30", "15h30_15h45", "15h45_16h00", "16h00_16h15", "16h15_16h30", "16h30_16h45", "16h45_17h00", "17h00_17h15", "17h15_17h30", "17h30_17h45", "17h45_18h00", "18h00_18h15", "18h15_18h30", "18h30_18h45", "18h45_19h00", "19h00_19h15", "19h15_19h30", "19h30_19h45", "19h45_20h00", "20h00_20h15", "20h15_20h30", "20h30_20h45", "20h45_21h00", "21h00_21h15", "21h15_21h30", "21h30_21h45", "21h45_22h00", "22h00_22h15", "22h15_22h30", "22h30_22h45", "22h45_23h00", "23h00_23h15", "23h15_23h30", "23h30_23h45", "23h45_24h00"];

    static String timeSlice = "quarter";
    static String receiversMethod = "closest"  // random, closest
    static String ignoreAgents = ""

    public static void main(String[] args) {
        runChampignyEntd100p()
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
        doImportData = false;

        doExportRoads = false;
        doExportBuildings = false;

        doTrafficSimulation = false;

        // all flags inside doTrafficSimulation
        doImportMatsimTraffic = false;
        doCreateReceiversFromMatsim = false;
        doCalculateNoisePropagation = false;
        doCalculateNoiseMap = false;
        doCalculateExposure = false;

        doCreateReceiversGrid = false;
        doCalculateContourPropagation = false;
        doCalculateContourMaps = false;

        doExportContoursMaps = false;

        doExportResults = false;

        doChantierSimulation = true;

        doChantierSimulationPropagation = false;
        doChantierSimulationEmission = false;
        doChantierSimulationNoiseMap = false;
        doChantierSimulationNoiseMapContours = false;
        doExportContoursMapsConstruction = true;

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
            new Import_File().exec(connection, [
                    "tableName" : "CONSTRUCTION_SOURCES",
                    "inputSRID" : srid,
                    "pathFile": Paths.get(inputsFolder, "CONSTRUCTION_SOURCES.shp")
            ])
            Sql sql = new Sql(connection)
            sql.execute("DROP TABLE IF EXISTS CONSTRUCTION_LW")
            sql.execute("CREATE TABLE CONSTRUCTION_LW AS SELECT * FROM CSVREAD('" + Paths.get(inputsFolder, "CONSTRUCTION_LW.csv") + "');")
            sql.execute("DROP TABLE IF EXISTS CONSTRUCTION_PLANNING")
            sql.execute("CREATE TABLE CONSTRUCTION_PLANNING AS SELECT * FROM CSVREAD('" + Paths.get(inputsFolder, "CONSTRUCTION_PLANNING.csv") + "');")
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
                        "timeSlice"        : timeSlice, // DEN, hour, quarter
                        "skipUnused"       : "true",
                        "exportTraffic"    : "true",
                        "SRID"             : srid,
                        "ignoreAgents"     : ignoreAgents,
                        "perVehicleLevel"  : true,
                        "populationFactor" : populationFactor
                ]);
                Create0dBSourceFromRoads.create0dBSourceFromRoads(connection, [
                        "roadsTableName"  : "MATSIM_ROADS",
                        "sourcesTableName": "SOURCES_0DB"
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
            if (doCreateReceiversGrid) {
                new Delaunay_Grid().exec(connection, [
                        "outputTableName": "CONTOURS_RECEIVERS",
                        "tableBuilding": "BUILDINGS",
                        "sourcesTableName": "MATSIM_ROADS"
                ])
            }

            if (doCalculateNoisePropagation) {
                CalculateNoiseMapFromSource.calculateNoiseMap(connection, [
                        "tableBuilding"     : "BUILDINGS",
                        "tableReceivers"    : "ACTIVITIES_RECEIVERS",
                        "tableSources"      : "SOURCES_0DB",
                        "confMaxSrcDist"    : 250,
                        "confMaxReflDist"   : 25,
                        "confReflOrder"     : 1,
                        "confSkipLevening"  : true,
                        "confSkipLnight"    : true,
                        "confSkipLden"      : true,
                        "confThreadNumber"  : 16,
                        "confExportSourceId": true,
                        "confDiffVertical"  : false,
                        "confDiffHorizontal": true
                ]);
                new Sql(connection).execute("ALTER TABLE LDAY_GEOM RENAME TO ATTENUATION_TRAFFIC")
            }
            if (doCalculateContourPropagation) {
                CalculateNoiseMapFromSource.calculateNoiseMap(connection, [
                        "tableBuilding"     : "BUILDINGS",
                        "tableReceivers"    : "CONTOURS_RECEIVERS",
                        "tableSources"      : "SOURCES_0DB",
                        "confMaxSrcDist"    : 250,
                        "confMaxReflDist"   : 25,
                        "confReflOrder"     : 1,
                        "confSkipLevening"  : true,
                        "confSkipLnight"    : true,
                        "confSkipLden"      : true,
                        "confThreadNumber"  : 16,
                        "confExportSourceId": true,
                        "confDiffVertical"  : false,
                        "confDiffHorizontal": true
                ]);
                new Sql(connection).execute("ALTER TABLE LDAY_GEOM RENAME TO ATTENUATION_TRAFFIC_CONTOURS")
            }
            if (doCalculateNoiseMap) {
//                CalculateNoiseMapFromAttenuation.calculateNoiseMap(connection, [
//                        "matsimRoads"     : "MATSIM_ROADS",
//                        "matsimRoadsStats": "MATSIM_ROADS_STATS",
//                        "attenuationTable": "LDAY_GEOM",
//                        "outTableName"    : "RESULT_GEOM",
//                ])
                Sql sql = new Sql(connection);

                String outTableName = "RESULT_GEOM"
                String attenuationTable = "ATTENUATION_TRAFFIC"
                String matsimRoads = "MATSIM_ROADS"
                String matsimRoadsStats = "MATSIM_ROADS_STATS"

                sql.execute(String.format("DROP TABLE %s IF EXISTS", outTableName))
                String query = "CREATE TABLE " + outTableName + '''(
                        PK integer PRIMARY KEY AUTO_INCREMENT,
                        IDRECEIVER integer,
                        THE_GEOM geometry,
                        HZ63 double precision,
                        HZ125 double precision,
                        HZ250 double precision,
                        HZ500 double precision,
                        HZ1000 double precision,
                        HZ2000 double precision,
                        HZ4000 double precision,
                        HZ8000 double precision,
                        TIMESTRING varchar
                    )
                '''
                sql.execute(query)
                PreparedStatement insert_stmt = connection.prepareStatement(
                        "INSERT INTO " + outTableName + " VALUES(DEFAULT, ?, ST_GeomFromText(?, " + srid + "), ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )

                ensureIndex(connection, attenuationTable, "IDSOURCE", false)
                ensureIndex(connection, attenuationTable, "IDRECEIVER", false)
                ensureIndex(connection, matsimRoads, "LINK_ID", false)
                ensureIndex(connection, matsimRoadsStats, "LINK_ID", false)
                ensureIndex(connection, matsimRoadsStats, "TIMESTRING", false)

                Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
                List<String> mrs_freqs = ["LW63", "LW125", "LW250", "LW500", "LW1000", "LW2000", "LW4000", "LW8000"]

                long count = 0, do_print = 1
                List<GroovyRowResult> receivers_res = sql.rows("SELECT * FROM ACTIVITIES_RECEIVERS");
//                List<GroovyRowResult> receivers_res = sql.rows("SELECT * FROM ACTIVITIES_RECEIVERS WHERE PK NOT IN (SELECT DISTINCT rg.IDRECEIVER FROM RESULT_GEOM rg) ");
                long nb_receivers = receivers_res.size()
                long start = System.currentTimeMillis();
                for (GroovyRowResult receiver: receivers_res) {
                    long receiver_id = receiver["PK"] as long;
                    Geometry receiver_geom = receiver["THE_GEOM"] as Geometry;
                    Map<String, List<Double>> levels = new HashMap<String, List<Double>>();
                    List<GroovyRowResult> sources_att_res = sql.rows(String.format("SELECT lg.* FROM %s lg WHERE lg.IDRECEIVER = %d", attenuationTable, receiver_id));
                    long nb_sources = sources_att_res.size();
                    if (nb_sources == 0) {
                        count++
                        continue
                    }
                    for (GroovyRowResult sources_att: sources_att_res) {
                        long source_id = sources_att["IDSOURCE"] as long;
                        List<Double> attenuation = [
                                sources_att["HZ63"] as double,
                                sources_att["HZ125"] as double,
                                sources_att["HZ250"] as double,
                                sources_att["HZ500"] as double,
                                sources_att["HZ1000"] as double,
                                sources_att["HZ2000"] as double,
                                sources_att["HZ4000"] as double,
                                sources_att["HZ8000"] as double,
                        ];
                        List<GroovyRowResult> roads_stats_res = sql.rows(String.format(
                                "SELECT mrs.* FROM %s mrs INNER JOIN %s mr ON mr.LINK_ID = mrs.LINK_ID WHERE mr.PK = %d",
                                matsimRoadsStats, matsimRoads, source_id));
                        for (GroovyRowResult roads_stats: roads_stats_res) {
                            String timestring = roads_stats["TIMESTRING"] as String
                            if (!levels.containsKey(timestring)) {
                                levels[timestring] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                            }
                            for (i in 0..<8) {
                                double new_level = (roads_stats[mrs_freqs[i]] as double) + attenuation[i];
                                levels[timestring][i] = 10 * Math.log10( Math.pow(10, levels[timestring][i] / 10) + Math.pow(10, new_level / 10))
                            }
                        }
                    }

                    for (String timestring: (timeSlice == "quarter" ? quarterHourTimeStrings : hourTimeStrings)) {
                        if (!levels.containsKey(timestring)) {
                            levels[timestring] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                        }
                        List<Double> ts_levels = levels[timestring]
                        insert_stmt.setLong(1, receiver_id)
                        insert_stmt.setString(2, receiver_geom.toText())
                        for (i in 0..<8) {
                            insert_stmt.setDouble(i+3, ts_levels[i])
                        }
                        insert_stmt.setString(11, timestring)
                        insert_stmt.execute()
                    }
                    if (count >= do_print) {
                        double elapsed = (System.currentTimeMillis() - start + 1) / 1000
                        logger.info(String.format("Processing Receiver %d (max:%d) - elapsed : %ss (%.1fit/s)",
                            count, nb_receivers, elapsed, count/elapsed))
                        do_print *= 2
                    }
                    count ++
                }

                String prefix = "HZ"
                sql.execute("ALTER TABLE " + outTableName + " ADD COLUMN LEQA float as 10*log10((power(10,(" + prefix + "63-26.2)/10)+power(10,(" + prefix + "125-16.1)/10)+power(10,(" + prefix + "250-8.6)/10)+power(10,(" + prefix + "500-3.2)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000+1.2)/10)+power(10,(" + prefix + "4000+1)/10)+power(10,(" + prefix + "8000-1.1)/10)))")
                sql.execute("ALTER TABLE " + outTableName + " ADD COLUMN LEQ float as 10*log10((power(10,(" + prefix + "63)/10)+power(10,(" + prefix + "125)/10)+power(10,(" + prefix + "250)/10)+power(10,(" + prefix + "500)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000)/10)+power(10,(" + prefix + "4000)/10)+power(10,(" + prefix + "8000)/10)))")

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
                        "timeSlice"             : timeSlice, // DEN, hour, quarter,
//                        "plotOneAgentId"        : 10,
                ])
            }
        }

        if (doCalculateContourMaps) {
//                CalculateNoiseMapFromAttenuation.calculateNoiseMap(connection, [
//                        "matsimRoads"     : "MATSIM_ROADS",
//                        "matsimRoadsStats": "MATSIM_ROADS_STATS",
//                        "attenuationTable": "LDAY_GEOM",
//                        "outTableName"    : "RESULT_GEOM",
//                ])
            Sql sql = new Sql(connection);

            String outTableName = "RESULT_GEOM_CONTOURS"
            String attenuationTable = "ATTENUATION_TRAFFIC_CONTOURS"
            String matsimRoads = "MATSIM_ROADS"
            String matsimRoadsStats = "MATSIM_ROADS_STATS"

            sql.execute(String.format("DROP TABLE %s IF EXISTS", outTableName))
            String query = "CREATE TABLE " + outTableName + '''(
                        PK integer PRIMARY KEY AUTO_INCREMENT,
                        IDRECEIVER integer,
                        THE_GEOM geometry,
                        HZ63 double precision,
                        HZ125 double precision,
                        HZ250 double precision,
                        HZ500 double precision,
                        HZ1000 double precision,
                        HZ2000 double precision,
                        HZ4000 double precision,
                        HZ8000 double precision,
                        TIMESTRING varchar
                    )
                '''
            sql.execute(query)
            PreparedStatement insert_stmt = connection.prepareStatement(
                    "INSERT INTO " + outTableName + " VALUES(DEFAULT, ?, ST_GeomFromText(?, " + srid + "), ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )

            ensureIndex(connection, attenuationTable, "IDSOURCE", false)
            ensureIndex(connection, attenuationTable, "IDRECEIVER", false)
            ensureIndex(connection, matsimRoads, "LINK_ID", false)
            ensureIndex(connection, matsimRoadsStats, "LINK_ID", false)
            ensureIndex(connection, matsimRoadsStats, "TIMESTRING", false)

            Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
            List<String> mrs_freqs = ["LW63", "LW125", "LW250", "LW500", "LW1000", "LW2000", "LW4000", "LW8000"]

            long count = 0, do_print = 1
            List<GroovyRowResult> receivers_res = sql.rows("SELECT * FROM CONTOURS_RECEIVERS");
//                List<GroovyRowResult> receivers_res = sql.rows("SELECT * FROM ACTIVITIES_RECEIVERS WHERE PK NOT IN (SELECT DISTINCT rg.IDRECEIVER FROM RESULT_GEOM rg) ");
            long nb_receivers = receivers_res.size()
            long start = System.currentTimeMillis();
            for (GroovyRowResult receiver : receivers_res) {
                long receiver_id = receiver["PK"] as long;
                Geometry receiver_geom = receiver["THE_GEOM"] as Geometry;
                Map<String, List<Double>> levels = new HashMap<String, List<Double>>();
                List<GroovyRowResult> sources_att_res = sql.rows(String.format("SELECT lg.* FROM %s lg WHERE lg.IDRECEIVER = %d", attenuationTable, receiver_id));
                long nb_sources = sources_att_res.size();
                if (nb_sources == 0) {
                    count++
                    continue
                }
                for (GroovyRowResult sources_att : sources_att_res) {
                    long source_id = sources_att["IDSOURCE"] as long;
                    List<Double> attenuation = [
                            sources_att["HZ63"] as double,
                            sources_att["HZ125"] as double,
                            sources_att["HZ250"] as double,
                            sources_att["HZ500"] as double,
                            sources_att["HZ1000"] as double,
                            sources_att["HZ2000"] as double,
                            sources_att["HZ4000"] as double,
                            sources_att["HZ8000"] as double,
                    ];
                    List<GroovyRowResult> roads_stats_res = sql.rows(String.format(
                            "SELECT mrs.* FROM %s mrs INNER JOIN %s mr ON mr.LINK_ID = mrs.LINK_ID WHERE mr.PK = %d",
                            matsimRoadsStats, matsimRoads, source_id));
                    for (GroovyRowResult roads_stats : roads_stats_res) {
                        String timestring = roads_stats["TIMESTRING"] as String
                        if (!levels.containsKey(timestring)) {
                            levels[timestring] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                        }
                        for (i in 0..<8) {
                            double new_level = (roads_stats[mrs_freqs[i]] as double) + attenuation[i];
                            levels[timestring][i] = 10 * Math.log10(Math.pow(10, levels[timestring][i] / 10) + Math.pow(10, new_level / 10))
                        }
                    }
                }

                for (String timestring : (timeSlice == "quarter" ? quarterHourTimeStrings : hourTimeStrings)) {
                    if (!levels.containsKey(timestring)) {
                        levels[timestring] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                    }
                    List<Double> ts_levels = levels[timestring]
                    insert_stmt.setLong(1, receiver_id)
                    insert_stmt.setString(2, receiver_geom.toText())
                    for (i in 0..<8) {
                        insert_stmt.setDouble(i + 3, ts_levels[i])
                    }
                    insert_stmt.setString(11, timestring)
                    insert_stmt.execute()
                }
                if (count >= do_print) {
                    double elapsed = (System.currentTimeMillis() - start + 1) / 1000
                    logger.info(String.format("Processing Receiver %d (max:%d) - elapsed : %ss (%.1fit/s)",
                            count, nb_receivers, elapsed, count / elapsed))
                    do_print *= 2
                }
                count++
            }

            String prefix = "HZ"
            sql.execute("ALTER TABLE " + outTableName + " ADD COLUMN LEQA float as 10*log10((power(10,(" + prefix + "63-26.2)/10)+power(10,(" + prefix + "125-16.1)/10)+power(10,(" + prefix + "250-8.6)/10)+power(10,(" + prefix + "500-3.2)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000+1.2)/10)+power(10,(" + prefix + "4000+1)/10)+power(10,(" + prefix + "8000-1.1)/10)))")
            sql.execute("ALTER TABLE " + outTableName + " ADD COLUMN LEQ float as 10*log10((power(10,(" + prefix + "63)/10)+power(10,(" + prefix + "125)/10)+power(10,(" + prefix + "250)/10)+power(10,(" + prefix + "500)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000)/10)+power(10,(" + prefix + "4000)/10)+power(10,(" + prefix + "8000)/10)))")

        }

        if (doExportContoursMaps) {
//            new Export_Table().exec(connection, [
//                    "tableToExport": "CONTOURS_RECEIVERS",
//                    "exportPath"   : Paths.get(resultsFolder, "CONTOURS_RECEIVERS.geojson")
//            ])
//            new Export_Table().exec(connection, [
//                    "tableToExport": "TRIANGLES",
//                    "exportPath"   : Paths.get(resultsFolder, "TRIANGLES.geojson")
//            ])
            Sql sql = new Sql(connection)
            String dataTable = "RESULT_GEOM_CONTOURS"
            ensureIndex(connection, dataTable, "THE_GEOM", true)
            ensureIndex(connection, dataTable, "TIMESTRING", false)
            for (String timestring: (timeSlice == "quarter" ? quarterHourTimeStrings : hourTimeStrings)) {
                String timeDataTable = dataTable + "_" + timestring
                String contourDataTable = "CONTOURING_NOISE_MAP_" + timestring

                sql.execute(String.format("DROP TABLE %s IF EXISTS", timeDataTable))
                String query = "CREATE TABLE " + timeDataTable + '''(
                            PK integer PRIMARY KEY,
                            THE_GEOM geometry,
                            LAEQ double precision
                        )
                        AS SELECT r.IDRECEIVER as PK, r.THE_GEOM, r.LEQA as LAEQ
                        FROM ''' + dataTable +  " r WHERE r.TIMESTRING='" + timestring + "'"

                sql.execute(query)
                new Create_Isosurface().exec(connection, [
                    "resultTable": timeDataTable
                ])
                sql.execute(String.format("DROP TABLE %s IF EXISTS", contourDataTable))
                sql.execute("ALTER TABLE CONTOURING_NOISE_MAP RENAME TO " + contourDataTable)
                new Export_Table().exec(connection, [
                        "tableToExport": contourDataTable,
                        "exportPath"   : Paths.get(resultsFolder, contourDataTable + ".geojson")
                ])
                new Export_Table().exec(connection, [
                        "tableToExport": timeDataTable,
                        "exportPath"   : Paths.get(resultsFolder, timeDataTable + ".geojson")
                ])
            }
        }




        if (doChantierSimulation) {

            if (doChantierSimulationPropagation) {
                Create0dBSourceFromRoads.create0dBSourceFromRoads(connection, [
                        "roadsTableName"  : "CONSTRUCTION_SOURCES",
                        "sourcesTableName": "CONSTRUCTION_SOURCES_0DB"
                ]);
                CalculateNoiseMapFromSource.calculateNoiseMap(connection, [
                        "tableBuilding"     : "BUILDINGS",
                        "tableReceivers"    : "ACTIVITIES_RECEIVERS",
                        "tableSources"      : "CONSTRUCTION_SOURCES_0DB",
                        "confMaxSrcDist"    : 250,
                        "confMaxReflDist"   : 25,
                        "confReflOrder"     : 1,
                        "confSkipLevening"  : true,
                        "confSkipLnight"    : true,
                        "confSkipLden"      : true,
                        "confThreadNumber"  : 16,
                        "confExportSourceId": true,
                        "confDiffVertical"  : false,
                        "confDiffHorizontal": true
                ]);
                new Sql(connection).execute("DROP TABLE IF EXISTS ATTENUATION_CONSTRUCTION")
                new Sql(connection).execute("ALTER TABLE LDAY_GEOM RENAME TO ATTENUATION_CONSTRUCTION")
                CalculateNoiseMapFromSource.calculateNoiseMap(connection, [
                        "tableBuilding"     : "BUILDINGS",
                        "tableReceivers"    : "CONTOURS_RECEIVERS",
                        "tableSources"      : "CONSTRUCTION_SOURCES_0DB",
                        "confMaxSrcDist"    : 250,
                        "confMaxReflDist"   : 25,
                        "confReflOrder"     : 1,
                        "confSkipLevening"  : true,
                        "confSkipLnight"    : true,
                        "confSkipLden"      : true,
                        "confThreadNumber"  : 16,
                        "confExportSourceId": true,
                        "confDiffVertical"  : false,
                        "confDiffHorizontal": true
                ]);
                new Sql(connection).execute("DROP TABLE IF EXISTS ATTENUATION_CONSTRUCTION_CONTOURS")
                new Sql(connection).execute("ALTER TABLE LDAY_GEOM RENAME TO ATTENUATION_CONSTRUCTION_CONTOURS")
            }
            if (doChantierSimulationEmission) {
                Sql sql = new Sql(connection);

                String outTableName = "CONSTRUCTION_SOURCES_LW"
                String constructionSourcesTable = "CONSTRUCTION_SOURCES"
                String constructionPlanningTable = "CONSTRUCTION_PLANNING"
                String constructionLwTable = "CONSTRUCTION_LW"

                sql.execute(String.format("DROP TABLE %s IF EXISTS", outTableName))
                String query = "CREATE TABLE " + outTableName + '''(
                        PK integer PRIMARY KEY AUTO_INCREMENT,
                        IDSOURCE integer,
                        THE_GEOM geometry,
                        LW63 double precision,
                        LW125 double precision,
                        LW250 double precision,
                        LW500 double precision,
                        LW1000 double precision,
                        LW2000 double precision,
                        LW4000 double precision,
                        LW8000 double precision,
                        TIME int
                    )
                '''
                sql.execute(query)
                PreparedStatement insert_stmt = connection.prepareStatement(
                        "INSERT INTO " + outTableName + " VALUES(DEFAULT, ?, ST_GeomFromText(?, " + srid + "), ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )

                ensureIndex(connection, constructionSourcesTable, "PK", false)
                ensureIndex(connection, constructionPlanningTable, "SOURCE_ID", false)
                ensureIndex(connection, constructionPlanningTable, "POSITION_ID", false)
                ensureIndex(connection, constructionPlanningTable, "TIME", false)
                ensureIndex(connection, constructionLwTable, "SOURCE", false)

                Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
                List<String> lw_freqs = ["LW63", "LW125", "LW250", "LW500", "LW1000", "LW2000", "LW4000", "LW8000"]

                long count = 0, do_print = 1

                Map<String,List<Double>> machine_lw = new HashMap<String, List<Double>>();
                List<GroovyRowResult> machines_res = sql.rows("SELECT * FROM " + constructionLwTable);
                long nb_machines = machines_res.size()
                for (GroovyRowResult machine: machines_res) {
                    String machine_id = machine["SOURCE"] as String;
                    if (!machine_lw.containsKey(machine_id)) {
                        machine_lw[machine_id] = new ArrayList<Double>();
                    }
                    for (i in 0..<lw_freqs.size()) {
                        machine_lw[machine_id][i] = machine[lw_freqs[i]] as Double
                    }
                }

                List<GroovyRowResult> sources_res = sql.rows("SELECT * FROM " + constructionSourcesTable);
                long nb_sources = machines_res.size();
                long start = System.currentTimeMillis();
                for (GroovyRowResult source: sources_res) {
                    int position_id = source["PK"] as Integer;
                    Geometry position_geom = source["THE_GEOM"] as Geometry;
                    Map<Integer, List<Double>> levels = new HashMap<Integer, List<Double>>();
                    List<GroovyRowResult> planning_res = sql.rows(String.format("SELECT pl.* FROM %s pl WHERE pl.POSITION_ID = %d", constructionPlanningTable, position_id));

                    for (GroovyRowResult planning: planning_res) {
                        String machine_id = planning["SOURCE_ID"] as String
                        Integer time = planning["TIME"] as Integer
                        if (!levels.containsKey(time)) {
                            levels[time] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                        }
                        for (i in 0..<8) {
                            double new_level = machine_lw[machine_id][i]
                            levels[time][i] = 10 * Math.log10( Math.pow(10, levels[time][i] / 10) + Math.pow(10, new_level / 10))
                        }
                    }

                    for (int time = 0 ; time < 86400; time += (timeSlice == "quarter" ? 900 : 3600)) {
                        if (!levels.containsKey(time)) {
                            levels[time] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                        }
                        List<Double> ts_levels = levels[time]
                        insert_stmt.setLong(1, position_id)
                        insert_stmt.setString(2, position_geom.toText())
                        for (i in 0..<8) {
                            insert_stmt.setDouble(i+3, ts_levels[i])
                        }
                        insert_stmt.setInt(11, time)
                        insert_stmt.execute()
                    }
                    if (count >= do_print) {
                        double elapsed = (System.currentTimeMillis() - start + 1) / 1000
                        logger.info(String.format("Processing Receiver %d (max:%d) - elapsed : %ss (%.1fit/s)",
                                count, nb_sources, elapsed, count/elapsed))
                        do_print *= 2
                    }
                    count ++
                }
            }
            if (doChantierSimulationNoiseMap) {

                Sql sql = new Sql(connection);

                String outTableName = "RESULT_CONSTRUCTION"
                String attenuationTable = "ATTENUATION_CONSTRUCTION"
                String constructionSourceLwTable = "CONSTRUCTION_SOURCES_LW"

                sql.execute(String.format("DROP TABLE %s IF EXISTS", outTableName))
                String query = "CREATE TABLE " + outTableName + '''(
                        PK integer PRIMARY KEY AUTO_INCREMENT,
                        IDRECEIVER integer,
                        THE_GEOM geometry,
                        HZ63 double precision,
                        HZ125 double precision,
                        HZ250 double precision,
                        HZ500 double precision,
                        HZ1000 double precision,
                        HZ2000 double precision,
                        HZ4000 double precision,
                        HZ8000 double precision,
                        TIME int
                    )
                '''
                sql.execute(query)
                PreparedStatement insert_stmt = connection.prepareStatement(
                        "INSERT INTO " + outTableName + " VALUES(DEFAULT, ?, ST_GeomFromText(?, " + srid + "), ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )

                ensureIndex(connection, attenuationTable, "IDSOURCE", false)
                ensureIndex(connection, attenuationTable, "IDRECEIVER", false)
                ensureIndex(connection, constructionSourceLwTable, "IDSOURCE", false)

                Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
                List<String> lw_freqs = ["LW63", "LW125", "LW250", "LW500", "LW1000", "LW2000", "LW4000", "LW8000"]

                long count = 0, do_print = 1
                List<GroovyRowResult> receivers_res = sql.rows("SELECT * FROM ACTIVITIES_RECEIVERS");

                long nb_receivers = receivers_res.size()
                long start = System.currentTimeMillis();
                for (GroovyRowResult receiver: receivers_res) {
                    long receiver_id = receiver["PK"] as long;
                    Geometry receiver_geom = receiver["THE_GEOM"] as Geometry;
                    Map<Integer, List<Double>> levels = new HashMap<Integer, List<Double>>();
                    List<GroovyRowResult> sources_att_res = sql.rows(String.format("SELECT lg.* FROM %s lg WHERE lg.IDRECEIVER = %d", attenuationTable, receiver_id));
                    long nb_sources = sources_att_res.size();
                    if (nb_sources == 0) {
                        count++
                        continue
                    }
                    for (GroovyRowResult sources_att: sources_att_res) {
                        long source_id = sources_att["IDSOURCE"] as long;
                        List<Double> attenuation = [
                                sources_att["HZ63"] as double,
                                sources_att["HZ125"] as double,
                                sources_att["HZ250"] as double,
                                sources_att["HZ500"] as double,
                                sources_att["HZ1000"] as double,
                                sources_att["HZ2000"] as double,
                                sources_att["HZ4000"] as double,
                                sources_att["HZ8000"] as double,
                        ];
                        List<GroovyRowResult> source_lw_res = sql.rows(String.format(
                                "SELECT cslw.* FROM %s cslw WHERE cslw.IDSOURCE = %d",
                                constructionSourceLwTable, source_id));
                        for (GroovyRowResult source_lw: source_lw_res) {
                            Integer time = source_lw["TIME"] as Integer
                            if (!levels.containsKey(time)) {
                                levels[time] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                            }
                            for (i in 0..<8) {
                                double new_level = (source_lw[lw_freqs[i]] as double) + attenuation[i];
                                levels[time][i] = 10 * Math.log10( Math.pow(10, levels[time][i] / 10) + Math.pow(10, new_level / 10))
                            }
                        }
                    }

                    for (int time = 0 ; time < 86400; time += (timeSlice == "quarter" ? 900 : 3600)) {
                        if (!levels.containsKey(time)) {
                            levels[time] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                        }
                        List<Double> ts_levels = levels[time]
                        insert_stmt.setLong(1, receiver_id)
                        insert_stmt.setString(2, receiver_geom.toText())
                        for (i in 0..<8) {
                            insert_stmt.setDouble(i+3, ts_levels[i])
                        }
                        insert_stmt.setInt(11, time)
                        insert_stmt.execute()
                    }
                    if (count >= do_print) {
                        double elapsed = (System.currentTimeMillis() - start + 1) / 1000
                        logger.info(String.format("Processing Receiver %d (max:%d) - elapsed : %ss (%.1fit/s)",
                                count, nb_receivers, elapsed, count/elapsed))
                        do_print *= 2
                    }
                    count ++
                }

                String prefix = "HZ"
                sql.execute("ALTER TABLE " + outTableName + " ADD COLUMN LEQA float as 10*log10((power(10,(" + prefix + "63-26.2)/10)+power(10,(" + prefix + "125-16.1)/10)+power(10,(" + prefix + "250-8.6)/10)+power(10,(" + prefix + "500-3.2)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000+1.2)/10)+power(10,(" + prefix + "4000+1)/10)+power(10,(" + prefix + "8000-1.1)/10)))")
                sql.execute("ALTER TABLE " + outTableName + " ADD COLUMN LEQ float as 10*log10((power(10,(" + prefix + "63)/10)+power(10,(" + prefix + "125)/10)+power(10,(" + prefix + "250)/10)+power(10,(" + prefix + "500)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000)/10)+power(10,(" + prefix + "4000)/10)+power(10,(" + prefix + "8000)/10)))")
            }
            if (doChantierSimulationNoiseMapContours) {

                Sql sql = new Sql(connection);

                String outTableName = "RESULT_CONSTRUCTION_CONTOURS"
                String attenuationTable = "ATTENUATION_CONSTRUCTION_CONTOURS"
                String constructionSourceLwTable = "CONSTRUCTION_SOURCES_LW"

                sql.execute(String.format("DROP TABLE %s IF EXISTS", outTableName))
                String query = "CREATE TABLE " + outTableName + '''(
                        PK integer PRIMARY KEY AUTO_INCREMENT,
                        IDRECEIVER integer,
                        THE_GEOM geometry,
                        HZ63 double precision,
                        HZ125 double precision,
                        HZ250 double precision,
                        HZ500 double precision,
                        HZ1000 double precision,
                        HZ2000 double precision,
                        HZ4000 double precision,
                        HZ8000 double precision,
                        TIME int
                    )
                '''
                sql.execute(query)
                PreparedStatement insert_stmt = connection.prepareStatement(
                        "INSERT INTO " + outTableName + " VALUES(DEFAULT, ?, ST_GeomFromText(?, " + srid + "), ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )

                ensureIndex(connection, attenuationTable, "IDSOURCE", false)
                ensureIndex(connection, attenuationTable, "IDRECEIVER", false)
                ensureIndex(connection, constructionSourceLwTable, "IDSOURCE", false)

                Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
                List<String> lw_freqs = ["LW63", "LW125", "LW250", "LW500", "LW1000", "LW2000", "LW4000", "LW8000"]

                long count = 0, do_print = 1
                List<GroovyRowResult> receivers_res = sql.rows("SELECT * FROM CONTOURS_RECEIVERS");

                long nb_receivers = receivers_res.size()
                long start = System.currentTimeMillis();
                for (GroovyRowResult receiver: receivers_res) {
                    long receiver_id = receiver["PK"] as long;
                    Geometry receiver_geom = receiver["THE_GEOM"] as Geometry;
                    Map<Integer, List<Double>> levels = new HashMap<Integer, List<Double>>();
                    List<GroovyRowResult> sources_att_res = sql.rows(String.format("SELECT lg.* FROM %s lg WHERE lg.IDRECEIVER = %d", attenuationTable, receiver_id));
                    long nb_sources = sources_att_res.size();
                    if (nb_sources == 0) {
                        count++
                        continue
                    }
                    for (GroovyRowResult sources_att: sources_att_res) {
                        long source_id = sources_att["IDSOURCE"] as long;
                        List<Double> attenuation = [
                                sources_att["HZ63"] as double,
                                sources_att["HZ125"] as double,
                                sources_att["HZ250"] as double,
                                sources_att["HZ500"] as double,
                                sources_att["HZ1000"] as double,
                                sources_att["HZ2000"] as double,
                                sources_att["HZ4000"] as double,
                                sources_att["HZ8000"] as double,
                        ];
                        List<GroovyRowResult> source_lw_res = sql.rows(String.format(
                                "SELECT cslw.* FROM %s cslw WHERE cslw.IDSOURCE = %d",
                                constructionSourceLwTable, source_id));
                        for (GroovyRowResult source_lw: source_lw_res) {
                            Integer time = source_lw["TIME"] as Integer
                            if (!levels.containsKey(time)) {
                                levels[time] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                            }
                            for (i in 0..<8) {
                                double new_level = (source_lw[lw_freqs[i]] as double) + attenuation[i];
                                levels[time][i] = 10 * Math.log10( Math.pow(10, levels[time][i] / 10) + Math.pow(10, new_level / 10))
                            }
                        }
                    }

                    for (int time = 0 ; time < 86400; time += (timeSlice == "quarter" ? 900 : 3600)) {
                        if (!levels.containsKey(time)) {
                            levels[time] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                        }
                        List<Double> ts_levels = levels[time]
                        insert_stmt.setLong(1, receiver_id)
                        insert_stmt.setString(2, receiver_geom.toText())
                        for (i in 0..<8) {
                            insert_stmt.setDouble(i+3, ts_levels[i])
                        }
                        insert_stmt.setInt(11, time)
                        insert_stmt.execute()
                    }
                    if (count >= do_print) {
                        double elapsed = (System.currentTimeMillis() - start + 1) / 1000
                        logger.info(String.format("Processing Receiver %d (max:%d) - elapsed : %ss (%.1fit/s)",
                                count, nb_receivers, elapsed, count/elapsed))
                        do_print *= 2
                    }
                    count ++
                }

                String prefix = "HZ"
                sql.execute("ALTER TABLE " + outTableName + " ADD COLUMN LEQA float as 10*log10((power(10,(" + prefix + "63-26.2)/10)+power(10,(" + prefix + "125-16.1)/10)+power(10,(" + prefix + "250-8.6)/10)+power(10,(" + prefix + "500-3.2)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000+1.2)/10)+power(10,(" + prefix + "4000+1)/10)+power(10,(" + prefix + "8000-1.1)/10)))")
                sql.execute("ALTER TABLE " + outTableName + " ADD COLUMN LEQ float as 10*log10((power(10,(" + prefix + "63)/10)+power(10,(" + prefix + "125)/10)+power(10,(" + prefix + "250)/10)+power(10,(" + prefix + "500)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000)/10)+power(10,(" + prefix + "4000)/10)+power(10,(" + prefix + "8000)/10)))")
            }
        }
        if (doExportContoursMapsConstruction) {
            Sql sql = new Sql(connection)
            String dataTable = "RESULT_CONSTRUCTION_CONTOURS"
            ensureIndex(connection, dataTable, "THE_GEOM", true)
            ensureIndex(connection, dataTable, "TIME", false)
            for (int time = 0 ; time < 86400; time += (timeSlice == "quarter" ? 900 : 3600)) {
                String timestring = time as String
                String timeDataTable = dataTable + "_" + timestring
                String contourDataTable = "CONSTRUCTION_CONTOURING_NOISE_MAP_" + timestring

                sql.execute(String.format("DROP TABLE %s IF EXISTS", timeDataTable))
                String query = "CREATE TABLE " + timeDataTable + '''(
                            PK integer PRIMARY KEY,
                            THE_GEOM geometry,
                            LAEQ double precision
                        )
                        AS SELECT r.IDRECEIVER as PK, r.THE_GEOM, r.LEQA as LAEQ
                        FROM ''' + dataTable +  " r WHERE r.TIME='" + time + "'"

                sql.execute(query)
                new Create_Isosurface().exec(connection, [
                        "resultTable": timeDataTable
                ])
                sql.execute(String.format("DROP TABLE %s IF EXISTS", contourDataTable))
                sql.execute("ALTER TABLE CONTOURING_NOISE_MAP RENAME TO " + contourDataTable)
                new Export_Table().exec(connection, [
                        "tableToExport": contourDataTable,
                        "exportPath"   : Paths.get(resultsFolder, contourDataTable + ".geojson")
                ])
                new Export_Table().exec(connection, [
                        "tableToExport": timeDataTable,
                        "exportPath"   : Paths.get(resultsFolder, timeDataTable + ".geojson")
                ])
            }
        }



        if (doExportResults) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "RESULT_GEOM",
                    "exportPath"   : Paths.get(resultsFolder, "RESULT_GEOM.shp")
            ])
            ExportTable.exportTable(connection, [
                    "tableToExport": "RESULT_CONSTRUCTION",
                    "exportPath"   : Paths.get(resultsFolder, "RESULT_CONSTRUCTION.shp")
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

    static String time2timestring(int time, int timeSpan) {
        int hour = (time / 60 / 60) % 24
        int minute = (time / 60) % 60
        String start, end;
        if (minute == 0) {
            start = (hour as String)
        }
        else {
            start = (hour as String) + "h" + (minute as String)
        }
        int end_time = time + timeSpan
        hour = (end_time / 60 / 60) % 24
        minute = (end_time / 60) % 60

        if (minute == 0) {
            end = (hour as String);
        }
        else {
            end = (hour as String) + "h" + (minute as String);
        }
        return start + "_" + end;
    }
    static int timestring2time(String timestring) {
        String start_timestring = timestring.split("_")[0];
        List<String> hour_min = timestring.split("h");
        String hour = hour_min[0];
        String minute = "0";
        if (hour_min.size() == 2) {
            minute = hour_min[1];
        }
        return ((hour as int) * 60 * 60 + (minute as int) * 60)
    }

}
