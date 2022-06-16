package org.noise_planet.noisemodelling.work

import org.h2gis.functions.factory.H2GISDBFactory
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_File

import java.sql.Connection

class ImportFile {

    public static void main(String[] args) {

        Connection connection;
        String dbName = "file:///E:/EQLIFE/Scenarios/Barcelona/results.db"

        connection = JDBCUtilities.wrapConnection(H2GISDBFactory.openSpatialDataBase(dbName));
        importFile(connection);
    }

    public static void importFile(Connection connection) {
        importFile(connection, [
                "tableName" : "RECEIVERS_NEW",
                "inputSRID" : "2062",
                "pathFile": "E:\\EQLIFE\\Scenarios\\Barcelona\\RECEIVERS.shp"
        ])
    }
    public static void importFile(Connection connection, options) {
        println "-------------------------------"
        println "Importing File " + options.get("tableName")
        println "-------------------------------"
        new Import_File().exec(connection, options)
    }
}
