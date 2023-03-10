package org.noise_planet.noisemodelling.work

import groovy.sql.Sql
import org.h2.Driver
import org.h2gis.functions.factory.H2GISDBFactory
import org.h2gis.functions.factory.H2GISFunctions
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.noisemodelling.wps.Experimental_Matsim.Noise_From_Attenuation_Matrix

import java.sql.Connection
import java.sql.DriverManager

class CalculateNoiseMapFromAttenuation {

    public static void main(String[] args) {
        Connection connection;
        String dbName = "file:///D:/SYMEXPO/nm-symuvia-test/noisemodelling"
        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";AUTO_SERVER=TRUE";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);
        calculateNoiseMap(connection);
    }

    public static void calculateNoiseMap(Connection connection) {
        calculateNoiseMap(connection, [
            "matsimRoads": "LINK_POINT_SOURCES",
            "matsimRoadsStats": "SOURCES_LW",
            "attenuationTable" : "ATTENUATION_TRAFFIC",
            "outTableName" : "RESULT_GEOM",
            "timeBinSize": 900,
//            "timeBin": 3600,
        ])
    }

    public static void calculateNoiseMap(Connection connection, options) {
        println "-------------------------------"
        println "Calculate Noise Map From Attenuation Matrice - " + options.get("timeString")
        println "-------------------------------"
        new Noise_From_Attenuation_Matrix().exec(connection, options)
    }
}

