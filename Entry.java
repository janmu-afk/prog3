import java.io.IOException;

public class Entry {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println(
        "\nPROGRAM mode depth\nmode: 1 - single; 2 - multi; 3 - distrib\ndepth - depth of BFS traversal"
        );
        return; // exit in case of incorrect args
        }
        String[] arg = new String[1]; arg[0] = args[1];

        switch(Integer.parseInt(args[0])) {
            case 1:
                Main.main(arg);
                break;
            case 2:
                Multithreaded.main(arg);
                break;
            case 3:
                int cores = Runtime.getRuntime().availableProcessors() - 2;
                String depth = args[1];
                // replace .bat with .sh depending on shell in use
                ProcessBuilder pb = new ProcessBuilder("mpjrun.bat", "-np", String.valueOf(cores), "-dev", "multicore", "Distributed", depth);
                pb.inheritIO();

                try {
                    Process process = pb.start();
                    int exitCode = process.waitFor();
                    if (exitCode != 0) {
                        System.err.println("distrib mode failed: " + exitCode);
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
                
                break;
        }
    }
}
