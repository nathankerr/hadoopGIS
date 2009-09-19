package hadoopGIS;

import java.io.DataOutputStream;
import hadoopGIS.GIS;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import java.io.UnsupportedEncodingException;

public class GISRecordWriter<LongWritable, GIS> implements RecordWriter<LongWritable, GIS>
{
	DataOutputStream out;

	public GISRecordWriter(DataOutputStream out) {
		this.out = out;
	}

	public synchronized void write(LongWritable key, GIS value) throws IOException {
		out.writeBytes(value.toString());
	}

	public synchronized void close(Reporter reporter) throws IOException {
		out.close();
	}
}
