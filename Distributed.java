import java.io.*;
import java.util.*;
import mpi.MPI;

import utils.Util;

public class Distributed {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // initialize MPI
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        
        long startTimer = 0, endTimer;
        if (rank == 0) startTimer = System.currentTimeMillis();

        // prereqs
        int maxDepth = 4;
        // presumed distributed
        HashSet<String> blklistSet, BAYCSet = null, localBAYC;
        // 1. build the blacklist set
        // the master node will build the sets, others will receive them
        if (rank == 0) {
            blklistSet = Util.makeBlacklist("data/blacklist");
            // serialize the set
            byte[] blkBytes = Util.serialize(blklistSet);
            // broadcast its size
            MPI.COMM_WORLD.Bcast(new int[]{blkBytes.length}, 0, 1, MPI.INT, 0);
            // broadcast the data
            MPI.COMM_WORLD.Bcast(blkBytes, 0, blkBytes.length, MPI.BYTE, 0);

        } else {
            // individual values are in arrays because MPI is special
            int[] blkSize = new int[1];
            MPI.COMM_WORLD.Bcast(blkSize, 0, 1, MPI.INT, 0);
            // space out the data receiving
            byte[] blkBytes = new byte[blkSize[0]];
            // receive the data and rebroadcast it around
            MPI.COMM_WORLD.Bcast(blkBytes, 0, blkBytes.length, MPI.BYTE, 0);
            
            // deserialize the data (I know this is very wrong but casting is the best thing I could think of)
            // I would rather compromise the integrity of the code than compromise my sanity
            blklistSet = (HashSet<String>) Util.deserialize(blkBytes);
        }


        if (rank == 0) System.out.println("stage 1 complete");
        
        
        // 2. build the BAYC set
        // each node gets its chunk
        if (rank == 0) {
            BAYCSet = Util.makeBAYC("data/boredapeyachtclub.csv");
            // convert into list and calculate the distribution size
            ArrayList<String> baycList = new ArrayList<>(BAYCSet);
            int total = baycList.size();
            int perNode = (int) Math.ceil(total / (double) size);

            // for each node, get its limits and get a sublist
            for (int i = 1; i < size; i++) {
                int start = i * perNode;
                if (start >= total) continue;
                int end = Math.min(start + perNode, total);
                List<String> subList = baycList.subList(start, end);
                HashSet<String> partition = new HashSet<>(subList);
                byte[] serialized = Util.serialize(partition);
                MPI.COMM_WORLD.Send(new int[]{serialized.length}, 0, 1, MPI.INT, i, 0);
                MPI.COMM_WORLD.Send(serialized, 0, serialized.length, MPI.BYTE, i, 1);
            }

            // node 0 specific
            int end = Math.min(perNode, total);
            localBAYC = new HashSet<>(baycList.subList(0, end));
        } else {
            int[] lengthBuf = new int[1];
            MPI.COMM_WORLD.Recv(lengthBuf, 0, 1, MPI.INT, 0, 0);
            byte[] recvBuf = new byte[lengthBuf[0]];
            MPI.COMM_WORLD.Recv(recvBuf, 0, recvBuf.length, MPI.BYTE, 0, 1);
            localBAYC = (HashSet<String>) Util.deserialize(recvBuf);
        }


        HashMap<String, HashSet<String>> ETNChunk = new HashMap<>();

        // each node reads its own chunk from the ETN file
        try (BufferedReader reader = new BufferedReader(new FileReader("data/prog3ETNsample.csv"))) {
            String line;
            int lineNumber = 0;
            int totalLines = 1000000; // approximate total lines, adjust as needed
            int linesPerNode = (int) Math.ceil(totalLines / (double) size);
            int startLine = rank * linesPerNode;
            int endLine = Math.min(startLine + linesPerNode, totalLines);

            while ((line = reader.readLine()) != null) {
                if (lineNumber >= startLine && lineNumber < endLine) {
                    String[] columns = line.split(",");
                    String from = columns[5].trim();
                    String to = columns[6].trim();
                    if (blklistSet.contains(from) || blklistSet.contains(to)) continue;
                    ETNChunk.computeIfAbsent(from, k -> new HashSet<>()).add(to);
                }
                lineNumber++;
                if (lineNumber >= endLine) break;
            }
        }


        // 4. build the linkability network (implementation dependent)
        // first we need the master node to redistribute the entire BAYC set as well
        byte[] serializedBAYC = null;
        int[] lengthBuf = new int[1];
        if (rank == 0) {
            serializedBAYC = Util.serialize(BAYCSet);
            lengthBuf[0] = serializedBAYC.length;
        }
        MPI.COMM_WORLD.Bcast(lengthBuf, 0, 1, MPI.INT, 0);
        if (rank != 0) {
            serializedBAYC = new byte[lengthBuf[0]];
        }
        MPI.COMM_WORLD.Bcast(serializedBAYC, 0, lengthBuf[0], MPI.BYTE, 0);
        if (rank != 0) {
            BAYCSet = (HashSet<String>) Util.deserialize(serializedBAYC);
        }

        // now for the fun part
        // each node has access to the following:
            // full BAYC set: BAYCSet
            // partial BAYC set: localBAYC
            // full ETN chunk: ETNChunk
            // full blacklist: blklistSet
        HashMap<String, HashMap<String, Integer>> localLinkNet = new HashMap<>();
        for (String entry : localBAYC) {
            if (blklistSet.contains(entry)) continue;

            LinkedList<String> queue = new LinkedList<>();
            HashSet<String> visited = new HashSet<>();
            HashMap<String, Integer> depth = new HashMap<>();

            queue.add(entry);
            visited.add(entry);
            depth.put(entry, 0);

            while (!queue.isEmpty()) {
                String current = queue.poll();
                int currDepth = depth.get(current);

                if (currDepth == maxDepth) continue;

                HashSet<String> neighbors = ETNChunk.getOrDefault(current, new HashSet<>());
                for (String neighbor : neighbors) {
                    if (visited.contains(neighbor)) continue;
                    if (blklistSet.contains(neighbor)) continue;

                    int nextDepth = currDepth + 1;
                    visited.add(neighbor);
                    depth.put(neighbor, nextDepth);
                    queue.add(neighbor);

                    if (BAYCSet.contains(neighbor) && !neighbor.equals(entry)) {
                        localLinkNet.computeIfAbsent(entry, k -> new HashMap<>())
                                    .put(neighbor, nextDepth);
                    }
                }
            }
        }

        // now to redistribute the rewards
        byte[] serializedLocalLinkNet = Util.serialize(localLinkNet);
        int[] localSize = new int[]{serializedLocalLinkNet.length};

        int[] allSizes = new int[size];  // size = total MPI processes
        MPI.COMM_WORLD.Gather(localSize, 0, 1, MPI.INT, allSizes, 0, 1, MPI.INT, 0);

        byte[] recvBuffer = null;
        int[] displacements = null;
        
        // recalc the displacements
        if (rank == 0) {
            int totalSize = Arrays.stream(allSizes).sum();
            recvBuffer = new byte[totalSize];
            displacements = new int[size];
            displacements[0] = 0;
            for (int i = 1; i < size; i++) {
                displacements[i] = displacements[i - 1] + allSizes[i - 1];
            }
        }

        MPI.COMM_WORLD.Gatherv(serializedLocalLinkNet, 0, serializedLocalLinkNet.length, MPI.BYTE, recvBuffer, 0, allSizes, displacements, MPI.BYTE, 0);

        HashMap<String, HashMap<String, Integer>> globalLinkNet = new HashMap<>();

        if (rank == 0) {
            for (int i = 0; i < size; i++) {
                int offset = displacements[i];
                int len = allSizes[i];
                byte[] part = Arrays.copyOfRange(recvBuffer, offset, offset + len);

                HashMap<String, HashMap<String, Integer>> partial = (HashMap<String, HashMap<String, Integer>>) Util.deserialize(part);

                // merge partial into globalLinkNet
                for (var entry : partial.entrySet()) {
                    String from = entry.getKey();
                    HashMap<String, Integer> neighbors = entry.getValue();
                    globalLinkNet.computeIfAbsent(from, k -> new HashMap<>()).putAll(neighbors); // overwrites duplicates
                }
            }
        }


        // 5. writing to a csv file
        //get the current timestamp for the filename
        if (rank == 0) {
            long unixTimestamp = System.currentTimeMillis() / 1000L;
            // it's safer this way
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(unixTimestamp + ".csv"))) {
                writer.write("from,to,weight\n");
                // convert a hashmap into an entry for iteration purposes
                for (HashMap.Entry<String, HashMap<String, Integer>> fromEntry : globalLinkNet.entrySet()) {
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


        if (rank == 0) {
            endTimer = System.currentTimeMillis();
            System.out.println("\n" + (endTimer - startTimer) + "ms");
        }

    }
}