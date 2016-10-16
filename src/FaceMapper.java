import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.highgui.Highgui;
import org.opencv.objdetect.CascadeClassifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import utils.ArrayWritableLong;
import utils.ImgRecordReader;
import utils.InputFormatImg;

public class FaceMapper extends Mapper<LongWritable, BufferedImage 
, Text, IntWritable>{

private final static IntWritable one = new IntWritable
(1);

public void map(LongWritable key, BufferedImage value, Context context) 
		throws IOException, InterruptedException {

	 System.loadLibrary(Core.NATIVE_LIBRARY_NAME);

       
        System.out.println("\nRunning FaceDetector");
	CascadeClassifier faceDetector = new CascadeClassifier(FaceMapper.class.getResource("haarcascade_frontalface_alt.xml").getPath());
	
	byte[] pixels = ((DataBufferByte)value.getRaster().getDataBuffer())
            .getData();

    // Create a Matrix the same size of image
    Mat image = new Mat(value.getHeight(), value.getWidth(), CvType.CV_8UC3);
    // Fill Matrix with image values
    image.put(0, 0, pixels);

    MatOfRect faceDetections = new MatOfRect();
    faceDetector.detectMultiScale(image, faceDetections);

    int n=faceDetections.toArray().length;
    System.out.println(key+"  "+n);
    switch(n)
    {
    case 1:
    	context.write(new Text("1 person"), one);
	break;
	
    case 2:
    	context.write(new Text("2 person"), one);
	break;
    
    case 3:
    	context.write(new Text("3 person"), one);
	break;
	
    case 4:
    	context.write(new Text("4 person"), one);
	break;
	
	default:
	    	context.write(new Text("more than 4 person"), one);
	break;
    }
    }
			
}