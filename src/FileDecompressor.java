import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @Description 解压文件
 * @Author Stringing
 * @Date 2018/8/23 13:26
 */
public class FileDecompressor {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        //这个方法会根据输入流路径名判断返回的codec的类型
        CompressionCodec codec = factory.getCodec(inputPath);
        if(codec == null){
            System.err.println("No codec found for uri " + uri);
            System.exit(1);
        }
        //解压后的文件是去掉对应压缩后缀的
        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream in = null;
        OutputStream out = null;
        try{
            //创建读入压缩文件的输入流（读进一个压缩文件，就是要解压它了）
            in = codec.createInputStream(fs.open(inputPath));
            //输出流的输出路径就是同一目录下去掉了压缩后缀的文件
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        }finally {
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
        }
    }
}
