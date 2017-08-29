import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.net.URI;

/**
 * Created by gzhukov on 04/08/17.
 */
public class GetPathLen {
    public static String uri = "hdfs://localhost:8020/tmp/hive/spark/004f7834-ea2c-4daf-bda0-40a97bc158e1";
    public static FileSystem fs;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        System.out.println(fs.listStatus(path).length);

    }
}
