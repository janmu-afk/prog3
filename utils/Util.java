package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class Util {
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
}
