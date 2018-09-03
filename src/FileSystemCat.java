import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @Description 通过FileSystem API读取数据
 * @Author Stringing
 * @Date 2018/8/15 20:29
 */
public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        InputStream in = null;
        String uri = args[0];
        Configuration conf = new Configuration();
        try{
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
