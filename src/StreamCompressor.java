import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @Description 用codec压缩实现来压缩输入的数据并输出
 * @Author Stringing
 * @Date 2018/8/23 12:09
 */
public class StreamCompressor {
    public static void main(String[] args) throws ClassNotFoundException, IOException {
        //用户选择用哪种codec实现，比如gzipcodec或者bzip2codec等
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);

        Configuration conf = new Configuration();
        //获取对应的codec实例
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        //用于输出压缩过的数据的输出流（输出压缩文件）
        CompressionOutputStream out = codec.createOutputStream(System.out);

        //将系统输入流输入的数据压缩并在系统输出流输出
        //echo "Text" | hadoop StreamCompressor org.apache.hadoop.io.compress.GzipCodec | gunzip
        //指定用gzip实现的codec，"Text"文本作为输入流传给StreamCompressor，压缩后的数据通过输出流传给gunzip
        //gunzip将压缩数据解压，正确的话得到"Text"
        IOUtils.copyBytes(System.in, out, 4096, false);
        //完成压缩数据的写操作
        out.finish();


    }
}
