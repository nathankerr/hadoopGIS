package hadoopGIS.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import hadoopGIS.GIS;
import hadoopGIS.GISInputFormat;
import hadoopGIS.GISOutputFormat;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.filecache.DistributedCache;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.ParseException;

public class filter extends Configured implements Tool, Mapper<LongWritable, GIS, LongWritable, GIS>, Reducer<LongWritable, GIS, LongWritable, GIS>
{
	private Geometry box;

	// For Mapper interface
	public void map(LongWritable key, GIS value, OutputCollector<LongWritable, GIS> output, Reporter reporter) throws IOException
	{
		// Keep records that intersect with the box
		if(value.geometry.intersects(box)) {
			output.collect(key, value);
		}
	}

	// For Reducer interface
	public void reduce(LongWritable key, Iterator<GIS> values, OutputCollector<LongWritable, GIS> output, Reporter reporter)
	{
		while(values.hasNext()) {
			try {
				output.collect(key, values.next());
			} catch (IOException e) {}
		}
	}

	// For Mapper (via JobConfigurable) interface
	public void configure(JobConf job)
	{
		ArrayList<String> columns = new ArrayList<String>();

		// Create the column list that is used
		columns.add("id");
		columns.add("the_geom");
		columns.add("devtype");

		// Create the box
		try {
			box = new WKTReader().read("POLYGON((-112.0859375 33.4349975585938,-112.0859375 33.4675445556641,-112.059799194336 33.4675445556641,-112.059799194336 33.4349975585938,-112.0859375 33.4349975585938))");
		} catch(com.vividsolutions.jts.io.ParseException e) {}
	}

	// For Mapper (via Closeable) interface
	public void close() {}

	// For Tool interface
	public int run(String[] args) throws Exception
	{
		JobConf job = new JobConf(new Configuration(), this.getClass());

		GISInputFormat.setInputPaths(job, new Path("/user/alaster/gis/parcels.gis"));
		Path p = new Path ("/user/alaster/gis/parcels.names");
		DistributedCache.addCacheFile (p.toUri (), job);
		job.set ("columnNames", p.getName ());

		GISOutputFormat.setOutputPath(job, new Path("output"));

		job.setJobName("hadoopGIS.examples.filter");

		job.setMapperClass(this.getClass());
		//job.setCombinerClass(this.getClass());
		job.setReducerClass(this.getClass());
     
		job.setInputFormat(GISInputFormat.class);
		//job.setOutputFormat(TextOutputFormat.class);
		job.setOutputValueClass(GIS.class);
		job.setOutputFormat(GISOutputFormat.class);

		return JobClient.runJob(job).getJobState();
 	}

	// Hadoop runner requires this to be a static void!
	// Thus must use exit instead of return
	// Also must directly use the class name instead of figuring it out
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new hadoopGIS.examples.filter(), args));
	}
}
