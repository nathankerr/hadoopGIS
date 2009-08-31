package cloudgis;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import cloudgis.GISRecordReader;
import org.apache.hadoop.mapred.InputSplit;
import java.io.IOException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class GISInputFormat extends FileInputFormat<LongWritable, GIS> {

	// From FileInputFormat
	public RecordReader<LongWritable, GIS> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException
	{
		return new GISRecordReader(job, (FileSplit) split);
	}

}
