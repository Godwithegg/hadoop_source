package com.danhuang.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HDFSClient {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://hadoop102:9000");
		
		//1.获取hdfs客户端对象
//		FileSystem fs = FileSystem.get(conf);
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		
		//2.在hdfs上创建路径
		fs.mkdirs(new Path("/0622/dashen/banzhang"));
		
		//3.关闭资源
		fs.close();
		
		System.out.println("over");
	}
	
	//1.文件上传
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "2");
		//1.获取fs对象
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		
		//2.执行上传API
		fs.copyFromLocalFile(new Path("E:/java_workspace/woshibanzhang.txt"), new Path("/xiaohua.txt"));
		
		//3.关闭资源
		fs.close();
	}
	
	//2.文件下载
	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
		
		Configuration conf = new Configuration();
		//1.获取对象
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		
		//2.执行下载操作
//		fs.copyToLocalFile(new Path("/xiaohua.txt"), new Path("E:/java_workspace/banzhang.txt"));
		fs.copyToLocalFile(true, new Path("/banhua.txt"), new Path("E:/java_workspace/banzhang.txt"), true);
		
		//3.关闭资源
		fs.close();
	}
	
	//3.文件删除
	@Test
	public void teseDelete() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		//1.获取对象
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		
		//2.文件删除
		fs.delete(new Path("/0622"), true);
		
		//3.关闭资源
	}
	
	//4.文件更名
	@Test
	public void testRename() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		//1.获取对象
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		
		//2.文件重命名
		fs.rename(new Path("/banzhang.txt"), new Path("/duizhang.txt"));
		
		//3.关闭资源
		fs.close();
	}
	
	//5.文件详情查看
	@Test
	public void tesetListFiles() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		//1.获取对象
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		
		//2.查看文件详情
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			//查看文件名称，权限，长度，块信息
			System.out.println(fileStatus.getPath().getName());
			System.out.println(fileStatus.getPermission());
			System.out.println(fileStatus.getLen());
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("------------------");
		}
		
		
		
		//3.关闭资源
		fs.close();
	}
	
	//6.判断是文件还是文件夹
	@Test
	public void testListStatus() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		//1.获取对象
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "eggdan");
		
		//2.判断操作
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			if(fileStatus.isFile()) {
				//文件
				System.out.println("f:"+fileStatus.getPath().getName());
			} else {
				//文件夹
				System.out.println("d:"+fileStatus.getPath().getName());
			}
		}
		
		//3.关闭资源
		fs.close();
	}
}
