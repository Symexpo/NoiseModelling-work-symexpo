package org.noise_planet.noisemodelling.work

import org.h2gis.functions.factory.H2GISDBFactory
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table

import java.nio.file.Paths
import java.sql.Connection

class ExportTable {

    public static void main(String[] args) {

        Connection connection;
        String dbName = "file:///D:/SYMEXPO/matsim-nantes/edgt_20p/nantes_aire_urbaine/noisemodelling/noisemodelling"
        String resultsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_aire_urbaine\\noisemodelling\\results\\"

        connection = JDBCUtilities.wrapConnection(H2GISDBFactory.openSpatialDataBase(dbName));
        exportTable(connection, [
                "tableToExport": "MAP_BUILDINGS_GEOM",
                "exportPath"   : Paths.get(resultsFolder, "MAP_BUILDINGS_GEOM.geojson")
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
