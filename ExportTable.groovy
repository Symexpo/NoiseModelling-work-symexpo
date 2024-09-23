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
        String dbName = "file:///D:/SYMEXPO/matsim-lyon/edgt_20p/L63V/noisemodelling/noisemodelling"
        String resultsFolder = "D:\\SYMEXPO\\matsim-lyon\\edgt_20p\\L63V\\noisemodelling\\results\\"

        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";AUTO_SERVER=TRUE";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);
        exportTable(connection, [
                "tableToExport": "EXPOSURES_SEQUENCE",
                "exportPath"   : Paths.get(resultsFolder, "EXPOSURES_SEQUENCE.geojson")
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
