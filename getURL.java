package JavaHDFS.JavaHDFS;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;

public class getURL{
    public static void main(String[] args) throws Exception {
    	String url[]= {"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2",
    			"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
    			"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
    			"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2",
    			"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2",
    			"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2"};
    	String dst = "hdfs://cshadoop1/user/pxk170930/Assignment1/book";
    	int i;
    	for(i=0;i<=5;i++)
    	{
    	URL obj1 = new URL (url[i]);
        InputStream in = null;
        try
        {
	        in=obj1.openStream();
	        Configuration conf = new Configuration();
	        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
	        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
	        FileSystem fs = FileSystem.get(URI.create(dst), conf);
	        OutputStream out = fs.create(new Path(dst+i+".bz2"), new Progressable() {
	          public void progress() {
	            System.out.print(".");
	          }
	        });
	        IOUtils.copyBytes(in, out, 4096, true);
        }
        finally 
        {
        IOUtils.closeStream(in);
        }
    	}
    }  
}