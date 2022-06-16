package org.noise_planet.noisemodelling.work

import org.h2gis.functions.factory.H2GISDBFactory
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM_Pbf

import java.sql.Connection

class ImportBuildings {

    public static void main(String[] args) {

        Connection connection;
        String dbName = "h2gisdb"
        connection = JDBCUtilities.wrapConnection(H2GISDBFactory.openSpatialDataBase(dbName));

        importBuildings(connection);
    }

    public static void importBuildings(Connection connection) {
        importBuildings(connection, [
                "pathFile"        : "/home/valoo/Projects/IFSTTAR/OsmMaps/nantes.pbf",
                "ignoreBuilding" : false,
                "ignoreGround": true,
                "ignoreRoads": true,
                "removeTunnels": false,
                "targetSRID"      : 2154
        ])
    }
    public static void importBuildings(Connection connection, options) {
        println "-------------------------------"
        println "Importing Buildings from Osm"
        println "-------------------------------"
        new Import_OSM_Pbf().exec(connection, options)
    }
}