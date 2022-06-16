package org.noise_planet.noisemodelling.work

import org.h2gis.functions.factory.H2GISDBFactory
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.noisemodelling.wps.Database_Manager.Clean_Database
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM_Pbf

import java.nio.file.Paths
import java.sql.Connection

class RunOsmImport {
    static Connection connection;

    static void main(String[] args) {

        String dbName = "file:///D:/SYMEXPO/matsim-nantes/edgt_20p/nantes_aire_urbaine/noisemodelling/test"
        String osmFile = "D:\\SYMEXPO\\osm_maps\\map.osm.pbf";
        String resultsFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_aire_urbaine\\noisemodelling\\results\\"
        String matsimFolder = "D:\\SYMEXPO\\matsim-nantes\\edgt_20p\\nantes_aire_urbaine\\simulation_output\\"
        String srid = 2154;
        double populationFactor = 0.20;

//        dbName = "c2b513ce291145baaddf8a2f646c4523"
        println dbName
        connection = JDBCUtilities.wrapConnection(H2GISDBFactory.createSpatialDataBase(dbName, true))

        new Clean_Database().exec(connection, [
                "areYouSure": "yes"
        ])
        new Import_OSM_Pbf().exec(connection, [
                "pathFile"        : osmFile,
                "targetSRID"      : srid,
                "ignoreGround": true,
                "ignoreBuilding": false,
                "ignoreRoads": true,
                "removeTunnels": false
        ]);
        ExportTable.exportTable(connection, [
                "tableToExport": "BUILDINGS",
                "exportPath"   : Paths.get(resultsFolder, "BUILDINGS.shp")
        ])
        connection.close()
    }
}
