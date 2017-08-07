package net.iponweb;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Date;

/**
 * Created by gzhukov on 04/08/17.
 */
public class HdfsTmpCleanup {
    public static FileSystem fs;
    public static long secCleanup;
    public static boolean recursive;
    public static final int MAX_DELETE_COUNT = 2000;
    private static long count = 0;

    private static class CliArguments {
        @Parameter(names = { "-path", "-p"}, description = "HDFS path")
        public String path;

        @Parameter(names = {"-ts"}, variableArity = true, description = "Cleanup before date.now() - ts")
        public long secCleanup = 86400; // 24h

        @Parameter(names = {"-recursive", "-r"}, description = "Recursive delete")
        public boolean recursive = false;

        @Parameter(names = {"-help", "-h", "--help"}, help = true, description = "Print this help message and exit")
        public boolean help;
    }

    public static void main(String[] args) throws Exception {
        CliArguments arguments = new CliArguments();
        JCommander commander = new JCommander();
        JCommander.newBuilder()
                .addObject(arguments)
                .build()
                .parse(args);
        commander.setProgramName("java -jar hdfs-cleanup.jar");
        if (arguments.help) {
            commander.usage();
            System.exit(0);
        }
        String uri = arguments.path;
        secCleanup = arguments.secCleanup;
        recursive = arguments.recursive;

        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(arguments.path);
        System.out.println("Listing files...");
        try {
            cleanup(path);
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void cleanup(Path path) throws Exception {
        RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listLocatedStatus(path);
        while (fileStatusIterator.hasNext()) {
            LocatedFileStatus stat = fileStatusIterator.next();
            if (isOutdated(secCleanup, stat.getModificationTime())) {
                if (recursive) {
                    fs.deleteOnExit(stat.getPath());
                    count++;
                    System.out.printf("%d: Deleting path %s recursively \t mtime: %s\n", count, stat.getPath(), new Date(stat.getModificationTime()).toString());
                } else if (stat.isFile()) {
                    System.out.printf("Deleting path (file) %s \t mtime: %s\n", stat.getPath(), new Date(stat.getModificationTime()).toString());
                    fs.delete(stat.getPath(), false);
                } else if (fs.listStatus(stat.getPath()).length == 0) {
                    System.out.format("Deleting path (dir) (len: %d) %s \t mtime: %s\n", fs.listStatus(stat.getPath()).length, stat.getPath(), new Date(stat.getModificationTime()));
                    fs.delete(stat.getPath(), true);
                } else {
                    cleanup(stat.getPath());
                    cleanup(path);
                }
            }
        }
    }

    public static boolean isOutdated(long secCleanup, long ts) {
        long diff = new Date().getTime() - ts;
        if (diff > (secCleanup * 1000)) {
            return true;
        }
        return false;
    }
}
