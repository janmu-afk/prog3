import java.util.*;
import java.util.concurrent.Callable;

public class LinkNetBuilder implements Callable <HashMap<String, HashMap<String, Integer>>>{

    // the linkability network construction is directly split based on BAYC
    private final HashSet<String> BAYCChunk;
    private final HashMap<String, HashSet<String>> ETNChunk;
    private final HashSet<String> blacklist;
    private final HashSet<String> BAYCSet; // needed for proper comparisons
    private final int maxDepth;

    public LinkNetBuilder(HashSet<String> BAYCChunk, HashSet<String> BAYCSet, HashMap<String, HashSet<String>> ETNChunk,
                           HashSet<String> blacklist, int maxDepth) {
        this.BAYCChunk = BAYCChunk;
        this.BAYCSet = BAYCSet;
        this.ETNChunk = ETNChunk;
        this.blacklist = blacklist;
        this.maxDepth = maxDepth;
    }

    @Override
    public HashMap<String, HashMap<String, Integer>> call() {
        HashMap<String, HashMap<String, Integer>> localNetwork = new HashMap<>();

        for (String entry : BAYCChunk) {
            if (blacklist.contains(entry)) continue;

            LinkedList<String> queue = new LinkedList<>();
            HashMap<String, Integer> depth = new HashMap<>();
            HashSet<String> visited = new HashSet<>();

            queue.add(entry);
            depth.put(entry, 0);
            visited.add(entry);

            while (!queue.isEmpty()) {
                String current = queue.poll();
                int currDepth = depth.get(current);

                if (currDepth == maxDepth) continue;

                HashSet<String> neighbors = ETNChunk.getOrDefault(current, new HashSet<String>());

                for (String neighbor : neighbors) {
                    if (visited.contains(neighbor) || blacklist.contains(neighbor)) continue;

                    int nextDepth = currDepth + 1;
                    depth.put(neighbor, nextDepth);
                    visited.add(neighbor);
                    queue.add(neighbor);

                    if (BAYCSet.contains(neighbor) && !neighbor.equals(entry)) {
                        localNetwork
                            .computeIfAbsent(entry, k -> new HashMap<>())
                            .put(neighbor, nextDepth);
                    }
                }
            }
        }
        // we're only building a local network, we needed the full BAYC set for proper comparison
        return localNetwork;
    }
}