package org.noise_planet.noisemodelling.work

import org.h2.Driver
import org.h2gis.functions.factory.H2GISDBFactory
import org.h2gis.functions.factory.H2GISFunctions
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.noisemodelling.wps.Experimental_Matsim.Receivers_From_Activities_Closest

import java.sql.Connection
import java.sql.DriverManager

class AssignClosestReceiversToActivities {

    public static void main(String[] args) {
        Connection connection;
        String dbName = "file:///D:/SYMEXPO/matsim-nantes/edgt_100p/nantes_aire_urbaine/noisemodelling/noisemodelling"

        File dbFile = new File(URI.create(dbName));
        String databasePath = "jdbc:h2:" + dbFile.getAbsolutePath() + ";";
        Driver.load();
        connection = DriverManager.getConnection(databasePath, "", "");
        H2GISFunctions.load(connection);

        activitiesClosestReceiver(connection);
    }

    public static void activitiesClosestReceiver(Connection connection) {
        activitiesClosestReceiver(connection, [
                "activitiesTable": "ACTIVITIES",
                "receiversTable": "RECEIVERS",
                "outTableName": "ACTIVITIES_RECEIVERS"
        ])
    }

    public static void activitiesClosestReceiver(Connection connection, options) {
        println "-------------------------------"
        println "Creating Receivers grid"
        println "-------------------------------"
        new Receivers_From_Activities_Closest().exec(connection, options)
    }
}
