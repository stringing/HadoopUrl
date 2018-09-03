import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @Description 列出文件
 * @Author Stringing
 * @Date 2018/8/18 14:32
 */
public class ListStatus {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path[] paths = new Path[args.length];
        for(int i = 0; i < paths.length; i++){
            paths[i] = new Path(args[i]);
        }
        FileStatus[] fss = fs.listStatus(paths);
        Path[] filePaths = FileUtil.stat2Paths(fss);
        for(Path p : filePaths){
            System.out.println(p);
        }
    }
}
