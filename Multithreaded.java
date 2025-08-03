import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import utils.Util;

public class Multithreaded {
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();

        
        // prereqs
        int threadCount = Runtime.getRuntime().availableProcessors() - 2; // let's not overload things
        int maxDepth = Integer.parseInt(args[0]);
        System.out.println("\nrunning multi at " + maxDepth + " depth with " + threadCount +" threads\n");
        // presumed multithreaded
        // 1. build the blacklist set
        HashSet<String> blklistSet = Util.makeBlacklist("data/blacklist");

        // 2. build the BAYC set
        HashSet<String> BAYCSet = Util.makeBAYC("data/boredapeyachtclub.csv");

        // 3. build ETN chunk as an adjacency list
        // not parallelized because of disk I/O
        HashMap<String, HashSet<String>> etnChunk = new HashMap<>();
        // open CSV
        BufferedReader reader = new BufferedReader(new FileReader("data/prog3ETNsample.csv"));
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
            etnChunk.computeIfAbsent(from, k -> new HashSet<>());
            // add the to column to the from key (if absent)
            if (!etnChunk.get(from).contains(to)) {
                etnChunk.get(from).add(to);
            }
        }
        reader.close();

        //4. build the linkability network (implementation dependent)
        // each thread works with part of the BAYC set
        // create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        // prepare a list of future linkability network chunks
        ArrayList<Future<HashMap<String, HashMap<String, Integer>>>> futures = new ArrayList<>();

        // do the partitioning and add to future list for tasks
        ArrayList<HashSet<String>> chunks = Util.setSplit(BAYCSet, threadCount);
        for (HashSet<String> chunk : chunks) {
            futures.add(executor.submit(new LinkNetBuilder(chunk, BAYCSet, etnChunk, blklistSet, maxDepth)));
        }

        // merge the linkability network chunks
        HashMap<String, HashMap<String, Integer>> linkabilityNetwork = new HashMap<>();
        for (Future<HashMap<String, HashMap<String, Integer>>> future : futures) {

            //bad practice, I know
            HashMap<String, HashMap<String, Integer>> partial = null;
            // acquire futures - this is necessary to prevent the compiler from whining
            try { partial = future.get(); } catch(Exception e) {System.out.println(e);}
            
            // append to final structure
            for (HashMap.Entry<String, HashMap<String, Integer>> entry : partial.entrySet()) {
                linkabilityNetwork
                    .computeIfAbsent(entry.getKey(), k -> new HashMap<>())
                    .putAll(entry.getValue());
            }
        }

        // good practice :)
        executor.shutdown();

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
        long end = System.currentTimeMillis();
        System.out.println("\n" + (end - start) + "ms");
    }
}