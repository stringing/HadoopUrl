import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @Description
 * @Author Stringing
 * @Date 2018/8/23 14:17
 */
public class PooledStreamCompressor {
    public static void main(String[] args) throws ClassNotFoundException, IOException {
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        Compressor compressor = null;
        try{
            //从压缩池拿一个压缩器,参数是告诉它压缩器类型
            compressor = CodecPool.getCompressor(codec);
            //用对应压缩器来压缩
            CompressionOutputStream out = codec.createOutputStream(System.out, compressor);
            IOUtils.copyBytes(System.in, out, 4096, false);
            out.finish();
        }finally {
            CodecPool.returnCompressor(compressor);
        }

    }
}
