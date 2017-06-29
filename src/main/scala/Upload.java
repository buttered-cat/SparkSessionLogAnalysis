import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Administrator on 2017/6/27.
 */
public class Upload {
    public static void main(String[] args) throws IOException {
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(config);

        Path src = new Path(args[0]);
        Path dst = new Path(args[1]);

        hdfs.copyFromLocalFile(src, dst);

        hdfs.close();
    }
}
