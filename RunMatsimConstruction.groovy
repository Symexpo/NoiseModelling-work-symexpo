package org.noise_planet.noisemodelling.work

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.h2.Driver
import org.h2gis.functions.factory.H2GISFunctions
import org.locationtech.jts.geom.Geometry
import org.noise_planet.noisemodelling.wps.Database_Manager.Clean_Database
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_File
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.*

class RunMatsimConstruction {

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
    static boolean doCreateReceiversFromMatsim = true;
    static boolean doCalculateNoisePropagation = true;
    static boolean doCalculateNoiseMap = true;
    static boolean doCalculateExposure = true;

    // all flags inside doChantierSimulation
    static boolean doChantierSimulationPropagation = true;
    static boolean doChantierSimulationEmission = true;
    static boolean doChantierSimulationNoiseMap = true;

    static boolean doCompareExposureWithConstruction = true

    static int timeBinSize = 3600;
    static String receiversMethod = "closest"  // random, closest
    static String ignoreAgents = ""

    // acoustic propagation parameters
    static boolean diffHorizontal = true
    static boolean diffVertical = false
    static int reflOrder = 1
    static int maxReflDist = 50
    static def maxSrcDist = 750

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

        timeBinSize = 900

        reflOrder = 1
        maxReflDist = 50
        maxSrcDist = 750
        diffHorizontal = true

        doCleanDB = false;
        doImportOSMPbf = false;
        doImportData = false;

        doExportRoads = false;
        doExportBuildings = false;

        doTrafficSimulation = false;

        // all flags inside doTrafficSimulation
        doImportMatsimTraffic = true;
        doCreateReceiversFromMatsim = true;
        doCalculateNoisePropagation = true;
        doCalculateNoiseMap = true;
        doCalculateExposure = true;

        doChantierSimulation = false;

        // all flags inside doChantierSimulation
        doChantierSimulationPropagation = true;
        doChantierSimulationEmission = true;
        doChantierSimulationNoiseMap = true;

        doCompareExposureWithConstruction = true

        doExportResults = true;

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
                    "pathFile": Paths.get(inputsFolder, "CONSTRUCTION_SOURCES.geojson")
            ])
            Sql sql = new Sql(connection)
            sql.execute("ALTER TABLE CONSTRUCTION_SOURCES RENAME COLUMN ID TO PK")
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
                new Sql(connection).execute("ALTER TABLE LDAY_GEOM RENAME TO ATTENUATION_CONSTRUCTION")
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

                    for (int time = 0 ; time < 86400; time += timeBinSize) {
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
                long start = System.currentTimeMillis()
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

                    for (int time = 0 ; time < 86400; time += timeBinSize) {
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

        if (doCompareExposureWithConstruction) {
            Sql sql = new Sql(connection);

            Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

            String outTableName = "EXPOSURES_CONSTRUCTION_SEQUENCE"
            String dataTable = "RESULT_CONSTRUCTION"
            String outPersonTableName = "EXPOSURES_CONSTRUCTION"
            String receiversTable = "ACTIVITIES_RECEIVERS"

            sql.execute(String.format("DROP TABLE %s IF EXISTS", outTableName))
            String query = "CREATE TABLE " + outTableName + '''(
                        PK integer PRIMARY KEY AUTO_INCREMENT,
                        PERSON_ID varchar,
                        TIME int,
                        MAIN_ACTIVITY_ID varchar,
                        MAIN_ACTIVITY_TYPE varchar,
                        MAIN_ACTIVITY_GEOM geometry,
                        TRAFFIC_LAEQ double,
                        CONSTRUCTION_LAEQ double,
                        TOTAL_LAEQ double,
                        DIFF_LAEQ double
                    )
                '''
            sql.execute(query)
            logger.info(query)
            PreparedStatement insert_stmt = connection.prepareStatement(
                    "INSERT INTO " + outTableName + " VALUES(DEFAULT, ?, ?, ?, ?, ST_GeomFromText(?, " + srid + "), ?, ?, ?, ?)",
            )

            sql.execute(String.format("DROP TABLE %s IF EXISTS", outPersonTableName))
            query = "CREATE TABLE " + outPersonTableName + '''(
                        PK integer PRIMARY KEY AUTO_INCREMENT,
                        PERSON_ID varchar,
                        TRAFFIC_LAEQ double,
                        CONSTRUCTION_LAEQ double,
                        TOTAL_LAEQ double,
                        DIFF_LAEQ double
                    )
                '''
            sql.execute(query)
            logger.info(query)
            PreparedStatement insert_person_stmt = connection.prepareStatement(
                    "INSERT INTO " + outPersonTableName + " VALUES(DEFAULT, ?, ?, ?, ?, ?)",
            )
            logger.info("ensureIndex EXPOSURES PERSON_ID")
            ensureIndex(connection, "EXPOSURES", "PERSON_ID", false)
            logger.info("ensureIndex EXPOSURES_SEQUENCE PERSON_ID")
            ensureIndex(connection, "EXPOSURES_SEQUENCE", "PERSON_ID", false)
            logger.info("ensureIndex dataTable IDRECEIVER")
            ensureIndex(connection, dataTable, "IDRECEIVER", false)
            logger.info("ensureIndex dataTable TIME")
            ensureIndex(connection, dataTable, "TIME", false)

            Map<String, Map<Integer, Double>> activitiesTimeSeries = new HashMap<String, Map<Integer, Double>>()
            logger.info("populate activities")
            List<GroovyRowResult> activities_results = sql.rows("SELECT DISTINCT es.MAIN_ACTIVITY_ID FROM EXPOSURES_SEQUENCE es")
            for (GroovyRowResult activity_res: activities_results) {
                String activityId = activity_res["MAIN_ACTIVITY_ID"] as String
                query = '''
                            SELECT D.LEQA, D.TIME
                            FROM ''' + dataTable + ''' D
                            INNER JOIN ''' + receiversTable + ''' R
                            ON D.IDRECEIVER = R.PK
                            WHERE R.FACILITY = \'''' + activityId + '''\'
                        '''
                Statement stmt = connection.createStatement();
                ResultSet result = stmt.executeQuery(query)
                Map<Integer, Double> timeSeries = new HashMap<Integer, Double>()
                while(result.next()) {
                    int timeBin = result.getInt("TIME")
                    Double value = result.getDouble("LEQA")
                    timeSeries.put(timeBin, value)
                }
                activitiesTimeSeries.put(activityId, timeSeries)
            }

            Map<String, List<Double>> personsExposure = new HashMap<String, List<Double>>()

            logger.info("SELECT DISTINCT PERSON_ID FROM EXPOSURES e")
            List<GroovyRowResult> persons_results = sql.rows("SELECT DISTINCT PERSON_ID FROM EXPOSURES e")


            int doprint = 1
            int counter = 0
            int nbTimeBins = (int) (86400 / timeBinSize)
            long start = System.currentTimeMillis()
            int nb_persons = persons_results.size()

            for (GroovyRowResult person_res: persons_results) {
                String person_id = person_res["PERSON_ID"] as String

                List<GroovyRowResult> sequence_results = sql.rows("SELECT e.* FROM EXPOSURES_SEQUENCE e WHERE e.PERSON_ID = '" + person_id + "'")
                int nb_sequence = sequence_results.size()
                for (GroovyRowResult sequence_res : sequence_results) {
                    int timeBin = sequence_res["TIME"] as int
                    String activity_id = sequence_res["MAIN_ACTIVITY_ID"] as String
                    String activity_type = sequence_res["MAIN_ACTIVITY_TYPE"] as String
                    String activity_geom = sequence_res["MAIN_ACTIVITY_GEOM"] as String
                    double traffic_laeq = sequence_res["LEVEL"] as double
                    traffic_laeq = Math.max(20.0, traffic_laeq)
                    double construction_laeq = -99.0
                    if (activitiesTimeSeries[activity_id][timeBin] != null) {
                        construction_laeq = activitiesTimeSeries[activity_id][timeBin]
                    }
                    double total_laeq = 10 * Math.log10(Math.pow(10, traffic_laeq / 10) + Math.pow(10, construction_laeq / 10))
                    double diff_laeq = total_laeq - traffic_laeq

                    if (!personsExposure.containsKey(person_id)) {
                        personsExposure.put(person_id, new ArrayList<Double>([traffic_laeq, construction_laeq]))
                    } else {
                        List<Double> laeqs = personsExposure[person_id]
                        laeqs[0] = 10 * Math.log10(Math.pow(10, laeqs[0] / 10) + (1 / nbTimeBins) * Math.pow(10, traffic_laeq / 10))
                        laeqs[1] = 10 * Math.log10(Math.pow(10, laeqs[1] / 10) + (1 / nbTimeBins) * Math.pow(10, construction_laeq / 10))
                    }

                    insert_stmt.setString(1, person_id)
                    insert_stmt.setInt(2, timeBin)
                    insert_stmt.setString(3, activity_id)
                    insert_stmt.setString(4, activity_type)
                    insert_stmt.setString(5, activity_geom)
                    insert_stmt.setDouble(6, traffic_laeq)
                    insert_stmt.setDouble(7, construction_laeq)
                    insert_stmt.setDouble(8, total_laeq)
                    insert_stmt.setDouble(9, diff_laeq)
                    insert_stmt.addBatch()
                }
                insert_stmt.executeBatch()
                if (counter >= doprint) {
                    doprint *= 2
                    double elapsed = (System.currentTimeMillis() - start + 1) / 1000
                    logger.info(String.format("Processing Person %d (max:%d) - elapsed : %ss (%.1fit/s)",
                            counter, nb_persons, elapsed, counter / elapsed))
                }
                counter++;
            }

            for (Map.Entry<String, List<Double>> person : personsExposure.entrySet()) {
                String person_id = person.getKey()
                List<Double> laeqs = person.getValue()
                double traffic_laeq = laeqs[0]
                double construction_laeq = laeqs[1]
                double total_laeq = 10 * Math.log10(Math.pow(10, traffic_laeq / 10) + Math.pow(10, construction_laeq / 10))
                double diff_laeq = total_laeq - traffic_laeq

                insert_person_stmt.setString(1, person_id)
                insert_person_stmt.setDouble(2, traffic_laeq)
                insert_person_stmt.setDouble(3, construction_laeq)
                insert_person_stmt.setDouble(4, total_laeq)
                insert_person_stmt.setDouble(5, diff_laeq)
                insert_person_stmt.addBatch()
            }
            insert_person_stmt.executeBatch()

        }

        if (doExportResults) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "RESULT_GEOM",
                    "exportPath"   : Paths.get(resultsFolder, "NOISE_LEVELS_TRAFFIC.geojson")
            ])
            ExportTable.exportTable(connection, [
                    "tableToExport": "RESULT_CONSTRUCTION",
                    "exportPath"   : Paths.get(resultsFolder, "NOISE_LEVELS_CONSTRUCTION.geojson")
            ])
            ExportTable.exportTable(connection, [
                    "tableToExport": "EXPOSURES",
                    "exportPath"   : Paths.get(resultsFolder, "EXPOSURES.geojson")
            ])
            ExportTable.exportTable(connection, [
                    "tableToExport": "EXPOSURES_SEQUENCE",
                    "exportPath"   : Paths.get(resultsFolder, "EXPOSURES_SEQUENCE.geojson")
            ])
            ExportTable.exportTable(connection, [
                    "tableToExport": "EXPOSURES_CONSTRUCTION",
                    "exportPath"   : Paths.get(resultsFolder, "EXPOSURES_CONSTRUCTION.csv")
            ])
            ExportTable.exportTable(connection, [
                    "tableToExport": "EXPOSURES_CONSTRUCTION_SEQUENCE",
                    "exportPath"   : Paths.get(resultsFolder, "EXPOSURES_CONSTRUCTION_SEQUENCE.geojson")
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
