import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * @Description 将本地文件复制到hdfs中
 * @Author Stringing
 * @Date 2018/8/18 13:51
 */
public class FileCopyWithProgress {
    public static void main(String[] args) throws IOException {
        String localSrc = args[0];
        String des = args[1];
        Configuration conf = new Configuration();
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        FileSystem fs = FileSystem.get(URI.create(des), conf);
        OutputStream os = fs.create(new Path(des), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });
        try{
            IOUtils.copyBytes(in, os, 4096, false);
        }finally {
            IOUtils.closeStream(os);
            IOUtils.closeStream(in);
        }
    }
}
