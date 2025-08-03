package utils;

import java.io.*;
import java.util.*;

public class Util {
    // this function takes the BAYC dataset and converts it into a HashSet
    public static HashSet<String> makeBAYC (String file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = "";
        boolean skipHeader = true;  // skipping the header descriptor
        HashSet<String> bayc = new HashSet<>();
        while ((line = reader.readLine()) != null) {
            if (skipHeader) { skipHeader = false; continue; }
            String[] data = line.split(","); // csv
            bayc.add(data[4].trim()); // the from column
            bayc.add(data[5].trim()); // the to column
        }
        reader.close();
        return bayc;
    }

    // conversion of a folder of blacklist .json files into a HashSet
    public static HashSet<String> makeBlacklist(String folderName) throws IOException {
        HashSet<String> blklist = new HashSet<>();
        // several files in the folder
        File folder = new File(folderName);
        // add them to an array
        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".json"));
        // for each blacklist file, import all entries into set
        for (File file : files) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = "";
            while ((line = reader.readLine()) != null) {
                line = line.trim().replace("\"", ""); // make it easier to parse, like a comma list
                if (!line.isEmpty() && !line.equals("[") && !line.equals("]")) {
                    blklist.add(line.replace(",", "")); // json
                }
            }
            reader.close();
        }
        return blklist;
    }

    // taking a set and splitting it into a list of sets
    public static ArrayList<HashSet<String>> setSplit(HashSet<String> set, int count) {
        ArrayList<HashSet<String>> sets = new ArrayList<>();
        // conversion for easier processing
        ArrayList<String> list = new ArrayList<>(set);
        int size = list.size() / count;

        for (int i = 0; i < count; i++) {
            int start = i * size;
            // if true, end of list, else multiplied by current index
            int end = (i == count - 1) ? list.size() : (i + 1) * size;
            sets.add(new HashSet<>(list.subList(start, end)));
        }

        return sets;
    }
}
