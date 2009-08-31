package cloudgis;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import cloudgis.GIS;
import java.io.IOException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapred.RecordWriter;

public class GISOutputFormat extends FileOutputFormat<LongWritable, GIS>
{

	public RecordWriter<LongWritable, GIS> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException
	{
		Path file = getTaskOutputPath(job, name);
		FileSystem fs = file.getFileSystem(job);
		FSDataOutputStream fileOut = fs.create(file, progress);
		return new GISRecordWriter<LongWritable, GIS>(fileOut);
	}

}
