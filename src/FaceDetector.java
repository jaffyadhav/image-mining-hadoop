import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.StringTokenizer;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.opencv.core.Core;

import utils.InputFormatImg;
import utils.ArrayWritableLong;
import utils.ImgRecordReader;

public class FaceDetector {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, 
		args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: FaceDetect <in> <out>");
			System.exit(2);
		}
		BufferedImage img = null;
		FileSystem dfs = FileSystem.get(new URI("hdfs://localhost:9000"),conf);
		Path dir = new Path(otherArgs[0]);
		FileStatus[] files = dfs.listStatus(dir);
		
		//String fname = null;
		conf.setInt("utils.imagerecordreader.overlapPixel", 0);
		Path fpath = null;

		for (FileStatus file: files) {
			if (file.isDir()) continue;
		
			fpath = file.getPath();
			//fname = fpath.getName();
			System.out.println(fpath);
		}

		Path outdir = new Path(otherArgs[1]);
		if (dfs.exists(outdir)) dfs.delete(outdir,true);

		Job job = new Job(conf, "Face Detection");
		
		job.setJarByClass(FaceDetector.class);
		job.setMapperClass(FaceMapper.class);
		//job.setCombinerClass(FaceReducer.class);
		//job.setReducerClass(FaceReducer.class);

		job.setInputFormatClass(InputFormatImg.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));//hdfs input path to be fetched from command line
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		System.exit(job.waitForCompletion(true) ? 0 : 1);	//exit when job is completed


	}
}


