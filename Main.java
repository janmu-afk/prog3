import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import utils.Util;

public class Main {

    public static void main(String[] args) throws IOException{

        // prereqs
        int depth = 5;

        // presumed singlethreaded
        // 1. build the blacklist set
        HashSet<String> blklistSet = Util.makeBlacklist("data/blacklist");

        // 2. build the BAYC set
        HashSet<String> BAYCSet = Util.makeBAYC("data/boredapeyachtclub.csv");

        // 3. build ETN chunk as an adjacency list (implementation dependent)
        HashMap<String, List<String>> etnChunk = new HashMap<>();
        // open CSV
        BufferedReader reader = new BufferedReader(new FileReader("prog3ETNsample.csv"));
        String line = "";
        // iterate through the ETN chunk
        while ((line = reader.readLine()) != null) {
            //extract entry
            String[] columns = line.split(","); // split the columns
            String from = columns[5].trim();
            String to = columns[6].trim();
            // is either of the columns in the blacklist?
            if (blklistSet.contains(from) || blklistSet.contains(to)) continue;
            // if true, add the from column and create an array list for it (if absent)
            etnChunk.computeIfAbsent(from, k -> new ArrayList<>());
            // add the to column to the from key (if absent)
            if (!etnChunk.get(from).contains(to)) {
                etnChunk.get(from).add(to);
            }
        }
        reader.close();
        
        // 4. build the linkability network (implementation dependent)
        Map<String, Map<String, Integer>> linkabilityNetwork = new HashMap<>();
        // iterate through the BAYC set
        for (String entry : BAYCSet) {
            
        }
    }
}