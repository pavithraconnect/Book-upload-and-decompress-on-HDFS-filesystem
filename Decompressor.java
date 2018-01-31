package JavaHDFS.JavaHDFS;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;


public class Decompressor {

	  public static void main(String[] args) throws Exception {
	    String uri = "hdfs://cshadoop1/user/pxk170930/Assignment1/book";
	    int i;
	    for(i=0;i<=5;i++)
	    {
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    Path inputPath = new Path(uri+i+".bz2");
	    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	    CompressionCodec codec = factory.getCodec(inputPath);
	    if (codec == null) {
	      System.err.println("No codec found for " + uri+i+".bz2");
	      System.exit(1);
	    }

	    String outputUri =
	    CompressionCodecFactory.removeSuffix(uri+i+".txt", codec.getDefaultExtension());
	    InputStream in = null;
	    OutputStream out = null;
	    try {
	      in = codec.createInputStream(fs.open(inputPath));
	      out = fs.create(new Path(outputUri));
	      IOUtils.copyBytes(in, out, conf);
	    }
	    
	    finally {
	    	 if(fs.exists(inputPath))
	 	    {
	 	    fs.delete(inputPath, true); //Delete existing file
	 	    }
	      IOUtils.closeStream(in);
	      IOUtils.closeStream(out);
	    }
	    }
	  }
	}
	// ^^ FileDecompressor

