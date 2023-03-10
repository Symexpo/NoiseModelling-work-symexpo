package org.noise_planet.noisemodelling.work

import org.h2.Driver
import org.h2gis.functions.factory.H2GISDBFactory
import org.h2gis.functions.factory.H2GISFunctions
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table

import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager

class ExportTable {

    public static void main(String[] args) {

        Connection connection;
        String dbName = "file:///D:/SYMEXPO/matsim-94/entd_100p/champigny/noisemodelling/noisemodelling"
        String resultsFolder = "D:\\SYMEXPO\\matsim-94\\entd_100p\\champigny\\noisemodelling\\results\\"

        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);
        exportTable(connection, [
                "tableToExport": "TIME_CONTOURING_NOISE_MAP",
                "exportPath"   : Paths.get(resultsFolder, "TIME_CONTOURING_NOISE_MAP.shp")
        ]);
    }

    public static void exportTable(Connection connection) {
        exportTable(connection, [
                "tableToExport" : "TMP_SCREENS_MERGE",
                // "exportPath": "C:\\Users\\valen\\Documents\\IFSTTAR\\Results\\receivers.shp"
                "exportPath": "/home/valoo/Projects/IFSTTAR/OsmMaps/nantes_line_merge.shp"
        ])
    }
    public static void exportTable(Connection connection, options) {
        println "-------------------------------"
        println "Exporting Table " + options.get("tableToExport")
        println "-------------------------------"
        new Export_Table().exec(connection, options)
    }
}
