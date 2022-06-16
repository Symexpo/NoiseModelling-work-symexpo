package org.noise_planet.noisemodelling.work

import org.matsim.api.core.v01.Id
import org.matsim.api.core.v01.network.Link
import org.matsim.api.core.v01.network.Network
import org.matsim.core.network.NetworkUtils
import org.matsim.core.network.io.MatsimNetworkReader

class FilterNetworkCSV {

    static void main(String[] args) {

        String projectDir = "/home/valoo/Projects/IFSTTAR/Scenarios/output_entd_1p/nantes_ile_1p/scenario_matsim/"
        String matsim_network_path = projectDir + "output_network.xml.gz"
        String csv_network_path = projectDir + "network.csv"
        String csv_new_network_path = projectDir + "network_cut.csv"

        Network network = NetworkUtils.createNetwork()
        MatsimNetworkReader reader = new MatsimNetworkReader(network);
        reader.readFile(matsim_network_path)

        ArrayList<String> linkIds = new ArrayList<String>()

        def netLinks = network.getLinks()
        for (Map.Entry<Id<Link>, Link> entry: netLinks.entrySet()) {
            Id<Link> linkId = entry.getKey();
            Link link = entry.getValue();
            linkIds.add(linkId.toString())
        }

        BufferedWriter csvWriter = new BufferedWriter((new FileWriter(csv_new_network_path)))
        BufferedReader csvReader = new BufferedReader(new FileReader(csv_network_path));
        String row;
        boolean firstRow = true
        while ((row = csvReader.readLine()) != null) {
            if (firstRow) {
                csvWriter.writeLine(row)
                firstRow = false
            }
            String[] data = row.split(",", 2);
            println data[0]
            if (linkIds.contains(data[0])) {
                csvWriter.writeLine(row)
            }
        }
        csvReader.close();
        csvWriter.close();
    }
}
