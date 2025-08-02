import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import utils.Util;

public class Main {

    public static void main(String[] args) throws IOException{

        // prereqs
        int maxDepth = 5;
        // presumed singlethreaded
        // 1. build the blacklist set
        HashSet<String> blklistSet = Util.makeBlacklist("data/blacklist");
        System.out.println("finished blacklist");
        // 2. build the BAYC set
        HashSet<String> BAYCSet = Util.makeBAYC("data/boredapeyachtclub.csv");
        System.out.println("finished bayc");
        // 3. build ETN chunk as an adjacency list (implementation dependent)
        HashMap<String, HashSet<String>> etnChunk = new HashMap<>();
        // open CSV
        BufferedReader reader = new BufferedReader(new FileReader("data/prog3ETNsample.csv"));
        String line = "";
        System.out.println("start building etn chunk");
        int count = 0;
        // iterate through the ETN chunk
        while ((line = reader.readLine()) != null) {
            //extract entry
            String[] columns = line.split(","); // split the columns
            String from = columns[5].trim();
            String to = columns[6].trim();
            // is either of the columns in the blacklist?
            if (blklistSet.contains(from) || blklistSet.contains(to)) continue;
            // if true, add the from column and create an array list for it (if absent)
            etnChunk.computeIfAbsent(from, k -> new HashSet<>());
            // add the to column to the from key (if absent)
            if (!etnChunk.get(from).contains(to)) {
                etnChunk.get(from).add(to);
            }
            count++;
            if (count % 100000 == 0) {
                System.out.println("Processed " + count + " lines");
            }
        }
        reader.close();
        System.out.println("finished etn chunk");
        // 4. build the linkability network (implementation dependent)
        // address, mapped to multiple addresses with distance weights
        HashMap<String, HashMap<String, Integer>> linkabilityNetwork = new HashMap<>();
        // iterate through the BAYC set
        for (String entry : BAYCSet) {
            // we have to purge the BAYC addresses too
            if (blklistSet.contains(entry)) continue;
            // create a queue, its depth and visited nodes (it's better to be explicit about parametrized structs)
            LinkedList<String> queue = new LinkedList<>();
            HashMap<String, Integer> depth = new HashMap<>();
            HashSet<String> visited = new HashSet<>();

            queue.add(entry);
            depth.put(entry, 0);
            visited.add(entry);
            // main part of the traversal
            while (!queue.isEmpty()) {
                // get the next address
                String current = queue.poll();
                int currDepth = depth.get(current);

                if (currDepth == maxDepth) continue;

                HashSet<String> neighbors = etnChunk.getOrDefault(current, new HashSet<String>());

                for (String neighbor : neighbors) {
                    if (visited.contains(neighbor)) continue;
                    if (blklistSet.contains(neighbor)) continue;

                    int nextDepth = currDepth + 1;
                    depth.put(neighbor, nextDepth);
                    visited.add(neighbor);
                    queue.add(neighbor);

                    if (BAYCSet.contains(neighbor) && !neighbor.equals(entry)) {
                        linkabilityNetwork
                            .computeIfAbsent(entry, k -> new HashMap<>())
                            .put(neighbor, nextDepth); // overwrite with shortest path
                    }
                }
            }
        }

        // 5. writing to a csv file
        //get the current timestamp for the filename
        long unixTimestamp = System.currentTimeMillis() / 1000L;
        // it's safer this way
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(unixTimestamp + ".csv"))) {
            writer.write("from,to,weight\n");
            // convert a hashmap into an entry for iteration purposes
            for (HashMap.Entry<String, HashMap<String, Integer>> fromEntry : linkabilityNetwork.entrySet()) {
                String from = fromEntry.getKey();
                for (HashMap.Entry<String, Integer> toEntry : fromEntry.getValue().entrySet()) {
                    String to = toEntry.getKey();
                    int weight = toEntry.getValue();
                    writer.write(from + "," + to + "," + weight + "\n");
                }
            }
        } catch(Exception e) {
            System.out.println(e);
        }

    }
}