package org.noise_planet.noisemodelling.work

import org.h2.Driver
import org.h2gis.functions.factory.H2GISFunctions
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_File

import java.sql.Connection
import java.sql.DriverManager

class ImportFile {

    public static void main(String[] args) {
        String dbName = "file:///D:/SYMEXPO/matsim-94/entd_100p/champigny/noisemodelling/noisemodelling"

        Connection connection;
        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);
        importFile(connection);
    }

    public static void importFile(Connection connection) {
        importFile(connection, [
                "tableName" : "CHANTIER_SOURCES",
                "inputSRID" : "2154",
                "pathFile": "D:\\SYMEXPO\\matsim-94\\entd_100p\\champigny\\noisemodelling\\inputs\\CHANTIER_SOURCES.shp"
        ])
    }
    public static void importFile(Connection connection, options) {
        println "-------------------------------"
        println "Importing File " + options.get("tableName")
        println "-------------------------------"
        new Import_File().exec(connection, options)
    }
}
