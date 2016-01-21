package edu.asu.cse512;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

public class GeoSpatialUtils{
	public  static void deleteHDFSFile(String fileName) {
		try{
			Configuration hadoopcfg = new Configuration();
			hadoopcfg.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			hadoopcfg.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
//			FileSystem  hdfs = FileSystem.get(URI.create("hdfs://master:54310"), hadoopcfg);
//			hdfs.delete(new Path(fileName), true);
		} 
		catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}