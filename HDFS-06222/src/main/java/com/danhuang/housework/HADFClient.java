package com.danhuang.housework;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HADFClient {
	
	@Test
	public void testCopyFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		
		FileInputStream fis = new FileInputStream(new File("E:/java_workspace/banzhang.txt"));
		FSDataOutputStream fos = fs.create(new Path("/0622/banzhang.txt"));
		IOUtils.copyBytes(fis, fos, conf);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
	@Test
	public void rename() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		fs.rename(new Path("/0622/meizi.txt"), new Path("/meizi.txt"));
		fs.close();
	}
	//下载文件
	@Test
	public void testPutFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		FileOutputStream fos = new FileOutputStream(new File("E:/java_workspace/hadoop-2.7.2.tar.gz.part1001"));
		byte[] buf = new byte[1024];
		for(int i=0;i<1024*128;i++) {
			fis.read(buf);
			fos.write(buf);
		}
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
		
	}
	//下载第二块文件
	@Test
	public void testPutFileSeek2() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		FileOutputStream fos = new FileOutputStream(new File("E:/java_workspace/hadoop-2.7.2.tar.gz.part1002"));
		
		fis.seek(1024*1024*128);
		IOUtils.copyBytes(fis, fos, conf);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
}
