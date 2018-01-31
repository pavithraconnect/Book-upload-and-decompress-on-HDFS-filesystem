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
    	String url[]= {"<url>/book1.bz2",
    			"<url>/book2.bz2",
    			"<url>/book3.bz2",
    			"<url>/book4.bz2",
    			};
    	String dst = "hdfs://cshadoop1/user/useraccount/Assignment1/book";
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
