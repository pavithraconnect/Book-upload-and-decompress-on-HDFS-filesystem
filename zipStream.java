package JavaHDFS.JavaHDFS;
import java.net.*;
import java.util.zip.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import java.io.*;
public class zipStream{
    public static void main(String[] args) throws Exception {
     String url= "https://corpus.byu.edu/wikitext-samples/text.zip";
     URL obj1 = new URL (url);
     String dst = "hdfs://cshadoop1/user/pxk170930/AssiCorp.txt";
     
       InputStream a = obj1.openStream();
       ZipInputStream in = new ZipInputStream(a);
       ZipEntry entry;
       try
       {
           Configuration conf = new Configuration();
           conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
           conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
           FileSystem fs = FileSystem.get(URI.create(dst), conf);
           OutputStream out = fs.create(new Path(dst), new Progressable() {
             public void progress() {
               System.out.print(".");
             }
           });
           
           byte[] b = new byte[4096];
           int len;
           while((entry=in.getNextEntry())!=null)
           {
            
            while((len=in.read(b))>0)
            {
                   
             out.write(b,0,len);
            }
            
           }
       }
       finally 
       {
        IOUtils.closeStream(in);
       }
    }
}