package org.noise_planet.noisemodelling.work

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.apache.commons.cli.*
import org.h2.Driver
import org.h2gis.functions.factory.H2GISFunctions
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Point
import org.locationtech.jts.index.strtree.GeometryItemDistance
import org.locationtech.jts.index.strtree.ItemBoundable
import org.locationtech.jts.index.strtree.ItemDistance
import org.locationtech.jts.index.strtree.STRtree
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceDynamic
import org.noise_planet.noisemodelling.wps.Acoustic_Tools.Create_Isosurface
import org.noise_planet.noisemodelling.wps.Database_Manager.Clean_Database
import org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_Symuvia
import org.noise_planet.noisemodelling.emission.RoadSourceParametersDynamic
import org.noise_planet.noisemodelling.wps.Receivers.Delaunay_Grid
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet

class RunSymuvia {

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

    // SYMUVIA
    static boolean doImportSymuvia = false;
    static boolean doSplitRoads = false;
    static boolean doAssignSources = false;

    // all flags inside doSimulation
    static boolean doCreateReceivers = true;
    static boolean doCalculateNoisePropagation = true;
    static boolean doCalculateNoiseMap = true;
    static boolean doContouringNoiseMap = true

    static int timeBinSize = 900
    static int timeBinMin = 0
    static int timeBinMax = 86400

    // acoustic propagation parameters
    static boolean diffHorizontal = true
    static boolean diffVertical = false
    static int reflOrder = 1
    static int maxReflDist = 50
    static def maxSrcDist = 750

    public static void main(String[] args) {
        runTest()
        // cli(args)
    }

    public static void runTest() {

        String dbName = "file:///D:/SYMEXPO/nm-symuvia-test/noisemodelling"
        String osmFile = "D:\\SYMEXPO\\osm_maps\\L63V.osm.pbf";
        String inputsFolder = "D:\\SYMEXPO\\nm-symuvia-test\\inputs\\"
        String resultsFolder = "D:\\SYMEXPO\\nm-symuvia-test\\results\\"
        String srid = 2154;

        timeBinSize = 1
        timeBinMin = 0
        timeBinMax = 50

        diffHorizontal = true
        diffVertical = false
        reflOrder = 1
        maxReflDist = 25
        maxSrcDist = 250

        doCleanDB = false;
        doImportOSMPbf = false;
        doImportSymuvia = false;
        doSplitRoads = false;
        doAssignSources = true;

        doExportRoads = false;
        doExportBuildings = false;

        doTrafficSimulation = true;

        doExportResults = false;


        // all flags inside doSimulation
        doCreateReceivers = false;
        doCalculateNoisePropagation = false;
        doCalculateNoiseMap = true;
        doContouringNoiseMap = true;

        run(dbName, osmFile, inputsFolder, resultsFolder, srid)
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

    public static void run(String dbName, String osmFile, String inputsFolder, String resultsFolder, String srid) {

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
            String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";AUTO_SERVER=TRUE";
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
                    "ignoreRoads": false,
                    "removeTunnels": false
            ]);
        }

        if (doImportSymuvia) {
            new Import_Symuvia().exec(connection, [
                    "pathFile"      : inputsFolder + "symuvia.xml",
                    "inputSRID"     : srid,
                    "tableName"     : "SYMUVIA",
            ]);
        }

        if (doImportData) {
            println "Nothing to import"
        }

        if (doExportRoads) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "ROADS",
                    "exportPath"   : Paths.get(resultsFolder, "ROADS.geojson")
            ])
        }
        if (doExportBuildings) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "BUILDINGS",
                    "exportPath"   : Paths.get(resultsFolder, "BUILDINGS.geojson")
            ])
        }

        int split_distance = 20

        if (doSplitRoads) {
            Sql sql = new Sql(connection)
            String roads_table = "ROADS"
            String outTableName = "LINK_POINT_SOURCES"
            sql.execute(String.format("DROP TABLE IF EXISTS %s", outTableName))
            sql.execute('''CREATE TABLE ''' + outTableName + ''' ( 
                THE_GEOM geometry
            ) AS SELECT THE_GEOM FROM 
                ST_Explode('(
                    SELECT ST_ToMultiPoint(ST_DENSIFY(ST_FORCE2D(r.THE_GEOM), ''' + split_distance + ''')) AS THE_GEOM FROM ''' + roads_table + ''' r
                )')
            ''')
            sql.execute("ALTER TABLE LINK_POINT_SOURCES ADD PK INT AUTO_INCREMENT PRIMARY KEY")
            sql.execute("UPDATE LINK_POINT_SOURCES SET THE_GEOM = ST_UpdateZ(THE_GEOM, 0.05)")
        }

        if (doAssignSources) {
            Sql sql = new Sql(connection);

            double temperature = 20
            String surface = "NL01"
            boolean stud = false
            double junc_dist = 200
            int junc_type = 1
            int acc_type= 1

            Map<Long, Map<Integer, List<Double>>> timeLinkPointSources = new HashMap<>();

            List<GroovyRowResult> source_points = sql.rows("SELECT * FROM LINK_POINT_SOURCES")
            STRtree rtree = new STRtree(source_points.size())
            for(def point: source_points) {
                long p_id = point["PK"] as Long
                Point p = point["THE_GEOM"] as Point
                if (p == null) {
                    continue
                }
                Tuple2<Long,Point> item = new Tuple2<>(p_id, p)
                rtree.insert(p.getEnvelopeInternal(), item)
            }

            List<GroovyRowResult> symuvia_rows = sql.rows("SELECT * FROM SYMUVIA_TRAJ")
            for (GroovyRowResult traj: symuvia_rows) {
                long vehicle_id = traj["ID_VEH"] as Long;
                String vehicle_type = traj["TYPE"] as String;
                double vehicle_acc = traj["ACC"] as double;
                double vehicle_speed = traj["SPEED"] as double;
                Point position = traj["THE_GEOM"] as Point;
                int time = traj["TIME"] as Integer;
                if (time < timeBinMin || time >= timeBinMax) {
                    continue
                }
                int time_bin = getTimeBin(time)

                int veh_type_int = 0

                switch(vehicle_type) {
                    case 'VL':
                        veh_type_int = 1
                        break
                    case 'PL':
                        veh_type_int = 3
                        break
                    case 'TypeTrolley':
                        veh_type_int = 3
                        break
                    case 'BUS':
                        veh_type_int = 3
                        break
                }

                List<Double> lw = new ArrayList<>()
                ArrayList<Integer> freq_list = [63, 125, 250, 500, 1000, 2000, 4000, 8000]

                for (def freq: freq_list) {
                    RoadSourceParametersDynamic rsParameters = new RoadSourceParametersDynamic(vehicle_speed, vehicle_acc, veh_type_int as String, acc_type, freq, temperature, surface as String, stud, junc_dist, junc_type, 0.0, vehicle_id as int)
                    rsParameters.setSlopePercentage(0)
                    lw.add(EvaluateRoadSourceDynamic.evaluate(rsParameters))
                }

                Tuple2[] closest_points = rtree.nearestNeighbour(position.getEnvelopeInternal(), position, new TupleItemDistance(), 2) as Tuple2[]
                if (closest_points.size() == 0) {
                    continue
                }
                if (closest_points.size() == 1) {
                    continue
                }
                List<Double> distances = new ArrayList<>()
                for (Tuple2 source_point: closest_points) {
                    Point geom = source_point.getSecond() as Point;
                    double distance = geom.distance(position);
                    distances.add(distance)
                }
                double sum_distances = distances.sum() as double
                for (Tuple2 source_point: closest_points) {
                    long point_id = source_point.getFirst() as Long;
                    Point geom = source_point.getSecond() as Point;
                    double distance = geom.distance(position);

                    if (!timeLinkPointSources.containsKey(point_id)) {
                        timeLinkPointSources[point_id] = new HashMap<Integer, List<Double>>()
                        for (int t = timeBinMin; t < timeBinMax; t += timeBinSize) {
                            timeLinkPointSources[point_id][t] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                        }
                    }
                    if (!timeLinkPointSources[point_id].containsKey(time_bin)) {
                        timeLinkPointSources[point_id][time_bin] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                    }
                    for (int freq_i = 0; freq_i < 8; freq_i++) {
                        timeLinkPointSources[point_id][time_bin][freq_i] = 10 * Math.log10(
                            Math.pow(10, timeLinkPointSources[point_id][time_bin][freq_i] / 10)
                          + (1 - distance / sum_distances) * (1 / timeBinSize) * Math.pow(10, lw[freq_i] / 10)
                        )
                    }
                }
            }
            String outTableName = "SOURCES_LW"
            sql.execute(String.format("DROP TABLE IF EXISTS %s", outTableName))

            sql.execute("CREATE TABLE " + outTableName + ''' (
                PK integer PRIMARY KEY AUTO_INCREMENT,
                SOURCE_ID long,
                TIME integer,
                LW63 real,
                LW125 real,
                LW250 real,
                LW500 real,
                LW1000 real,
                LW2000 real,
                LW4000 real,
                LW8000 real
            );''')
            PreparedStatement insert_stmt = connection.prepareStatement("INSERT INTO " + outTableName + " VALUES(" +
                    "DEFAULT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

            for (def point_source: timeLinkPointSources) {
                Long source_id = point_source.getKey()
                for (def time_lw: point_source.getValue()) {
                    insert_stmt.setLong(1, source_id)
                    int time = time_lw.getKey()
                    insert_stmt.setInt(2, time)
                    List<Double> lw = time_lw.getValue()
                    for (int i = 0; i < lw.size(); i++) {
                        insert_stmt.setDouble(i + 3, lw[i]);
                    }
                    insert_stmt.addBatch()
                }
                insert_stmt.executeBatch()
            }
        }

        if (doTrafficSimulation) {

            if (doCreateReceivers) {
                new Delaunay_Grid().exec(connection, [
                    "tableBuilding": "BUILDINGS",
                    "sourcesTableName": "ROADS",
                    "outputTableName": "ISO_RECEIVERS"
                ])
            }

            if (doCalculateNoisePropagation) {
                Create0dBSourceFromRoads.create0dBSourceFromRoads(connection, [
                        "roadsTableName"  : "LINK_POINT_SOURCES",
                        "sourcesTableName": "SOURCES_0DB"
                ]);
                CalculateNoiseMapFromSource.calculateNoiseMap(connection, [
                        "tableBuilding"     : "BUILDINGS",
                        "tableReceivers"    : "ISO_RECEIVERS",
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
                Sql sql = new Sql(connection)
                Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

                String matsimRoads = "LINK_POINT_SOURCES"
                String matsimRoadsLw = "SOURCES_LW"
                String attenuationTable = "ATTENUATION_TRAFFIC"
                String receiversTable = "ISO_RECEIVERS"
                String outTableName = "NOISE_LEVELS"

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
                        "INSERT INTO " + outTableName + " VALUES(DEFAULT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )

                logger.info("searching indexes on attenuation matrix ... ")
                ensureIndex(connection, attenuationTable, "IDSOURCE", false)
                ensureIndex(connection, attenuationTable, "IDRECEIVER", false)
                logger.info("searching indexes on traffic tables ... ")
                ensureIndex(connection, matsimRoads, "PK", false)
                ensureIndex(connection, matsimRoadsLw, "SOURCE_ID", false)
                ensureIndex(connection, matsimRoadsLw, "TIME", false)

                List<String> mrs_freqs = ["LW63", "LW125", "LW250", "LW500", "LW1000", "LW2000", "LW4000", "LW8000"]

                long count = 0, do_print = 1
                List<GroovyRowResult> receivers_res = sql.rows("SELECT * FROM " + receiversTable);
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
                        List<GroovyRowResult> roads_stats_res = sql.rows(String.format(
                                "SELECT mrs.* FROM %s mrs INNER JOIN %s mr ON mr.PK = mrs.SOURCE_ID WHERE mr.PK = %d",
                                matsimRoadsLw, matsimRoads, source_id));
                        for (GroovyRowResult roads_stats: roads_stats_res) {
                            int timeBin = roads_stats["TIME"] as int
                            if (!levels.containsKey(timeBin)) {
                                levels[timeBin] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                            }
                            for (i in 0..<8) {
                                double new_level = (roads_stats[mrs_freqs[i]] as double) + attenuation[i];
                                levels[timeBin][i] = 10 * Math.log10( Math.pow(10, levels[timeBin][i] / 10) + Math.pow(10, new_level / 10))
                            }
                        }
                    }

                    for (int timeBin = timeBinMin; timeBin < timeBinMax; timeBin += timeBinSize) {
                        if (!levels.containsKey(timeBin)) {
                            levels[timeBin] = [-99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0, -99.0] as List<Double>
                        }
                        List<Double> ts_levels = levels[timeBin]
                        insert_stmt.setLong(1, receiver_id)
                        insert_stmt.setString(2, receiver_geom.toText())
                        for (i in 0..<8) {
                            insert_stmt.setDouble(i+3, ts_levels[i])
                        }
                        insert_stmt.setInt(11, timeBin)
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

                logger.info('End : Noise_From_Attenuation_Matrix')
                String resultString = "Process done. Table of receivers " + outTableName + " created !"
                logger.info('Result : ' + resultString)

            }
        }
        if (doContouringNoiseMap) {

            Sql sql = new Sql(connection)

            String dataTable = "NOISE_LEVELS"
            String resultTable = "TIME_CONTOURING_NOISE_MAP"

            sql.execute(String.format("DROP TABLE %s IF EXISTS", resultTable))
            String create_query = "CREATE TABLE " + resultTable + '''(
                            PK integer PRIMARY KEY AUTO_INCREMENT,
                            CELL_ID integer,
                            THE_GEOM geometry,
                            ISOLVL integer,
                            ISOLABEL varchar,
                            TIME integer
                        )
                    '''
            sql.execute(create_query)

            ensureIndex(connection, dataTable, "THE_GEOM", true)
            for (int time = timeBinMin ; time < timeBinMax; time += timeBinSize) {
                String timeString = time.toString();
                String timeDataTable = dataTable + "_" + timeString

                sql.execute(String.format("DROP TABLE %s IF EXISTS", timeDataTable))
                String query = "CREATE TABLE " + timeDataTable + '''(
                            PK integer PRIMARY KEY AUTO_INCREMENT,
                            THE_GEOM geometry,
                            HZ63 double precision,
                            HZ125 double precision,
                            HZ250 double precision,
                            HZ500 double precision,
                            HZ1000 double precision,
                            HZ2000 double precision,
                            HZ4000 double precision,
                            HZ8000 double precision,
                            TIME integer,
                            LAEQ double precision,
                            LEQ double precision
                        )
                        AS SELECT r.IDRECEIVER as PK, r.THE_GEOM, r.HZ63, r.HZ125, r.HZ250, r.HZ500, r.HZ1000, r.HZ2000, r.HZ4000, r.HZ8000, r.TIME, r.LEQA as LAEQ, r.LEQ
                        FROM ''' + dataTable + " r WHERE r.TIME=" + time + "" // r.LEQA >= 0 AND

                sql.execute(query)
                new Create_Isosurface().exec(connection, [
                        "resultTable": timeDataTable
                ])
                sql.execute("INSERT INTO " + resultTable + "(CELL_ID, THE_GEOM, ISOLVL, ISOLABEL, TIME) SELECT cm.CELL_ID, cm.THE_GEOM, cm.ISOLVL, cm.ISOLABEL, " + time + " FROM CONTOURING_NOISE_MAP cm")
                sql.execute(String.format("DROP TABLE %s IF EXISTS", "CONTOURING_NOISE_MAP"))
                sql.execute(String.format("DROP TABLE %s IF EXISTS", timeDataTable))

                new Export_Table().exec(connection, [
                        "tableToExport": "TIME_CONTOURING_NOISE_MAP",
                        "exportPath"   : Paths.get(resultsFolder, "TIME_CONTOURING_NOISE_MAP.shp")
                ]);
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

    static int getTimeBin(int time) {
        if (time < timeBinMin || time >= timeBinMax) {
            throw new Exception(String.format("time %d out of bounds [%d,%d[", time, timeBinMin, timeBinMax))
        }
        int delta_time = time - timeBinMin;
        return timeBinMin + (int)(time / timeBinSize) * timeBinSize
    }

    static class TupleItemDistance implements ItemDistance {

        @Override
        double distance(ItemBoundable item1, ItemBoundable item2) {
            Tuple2<Long, Point> tuple = (Tuple2) item1.getItem()
            Point g1 = tuple.getSecond() as Point;
            Point g2 = item2.getItem() as Point;
            return g1.distance(g2);
        }
    }
}
