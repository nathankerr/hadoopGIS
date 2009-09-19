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

public class TestGIS extends Configured implements Tool, Mapper<LongWritable, GIS, LongWritable, LongWritable>, Reducer<LongWritable, LongWritable, LongWritable, LongWritable>
{
	HashMap<Integer, Geometry> C;
	HashMap<Integer, Geometry> I;
	HashMap<Integer, Geometry> G;

	String parcelDataFile;
	ArrayList<String> parcelColumnList;

	// For Mapper interface
	public void map(LongWritable key, GIS value, OutputCollector<LongWritable, LongWritable> output, Reporter reporter) throws IOException
	{
		double minDistance = Double.MAX_VALUE, currDistance;
		int closestParcel = -1;

	/*
		GIS myGIS;
		String line;
		ArrayList<String> parcelColumnList = new ArrayList<String> ();
		BufferedReader reader = new BufferedReader(new FileReader(parcelDataFile));
		while ((line = reader.readLine()) != null)
		{
			myGIS.update (new Text (line), parcelColumnList);

			currDistance = myGIS.geometry.distance (value.geometry);
			if (currDistance < minDistance)
			{
				minDistance = currDistance;
				closestParcel = myGIS.attributes.get("id");
			}
		}
	*/

		HashMap<Integer,Geometry> parcels = null;
		if (value.attributes.get ("devtype").equals ("C"))
			parcels = C;
		else if (value.attributes.get ("devtype").equals ("G"))
			parcels = G;
		else if (value.attributes.get ("devtype").equals ("I"))
			parcels = I;

		Iterator it = parcels.entrySet().iterator();
		while (it.hasNext())
		{
			Map.Entry entry = (Map.Entry) it.next();

			currDistance = value.geometry.distance ((Geometry) entry.getValue ());
			if (currDistance < minDistance)
			{
				minDistance = currDistance;
				closestParcel = ((Integer) entry.getKey ()).intValue ();
			}
		}

		LongWritable lngClosestParcel = new LongWritable (closestParcel);
		output.collect(key, lngClosestParcel);
	}

	// For Reducer interface
	public void reduce(LongWritable key, Iterator<LongWritable> values, OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
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
		String columnFilename = job.get ("parcelColumnNames");
		String dataFilename = job.get ("parcelData");
		Path[] distCacheFiles = new Path[0];
		try { distCacheFiles = DistributedCache.getLocalCacheFiles(job); }
		catch (IOException e)   { return; }

		parcelColumnList = new ArrayList<String>();
		G = new HashMap<Integer, Geometry>(); 
		C = new HashMap<Integer, Geometry>(); 
		I = new HashMap<Integer, Geometry>();

		BufferedReader reader = null;
		String line;
		for (int i=0; i<distCacheFiles.length; i++)
		{
			if (distCacheFiles [i].getName ().equals (columnFilename))
			{
				try
				{
					reader = new BufferedReader(new FileReader(distCacheFiles [i].toString ()));
					while ((line = reader.readLine()) != null)
						parcelColumnList.add (line);
				}
				catch (Exception e) { }
				break;
			}
		}

		GIS myGIS = new GIS ();
		for (int i=0; i<distCacheFiles.length; i++)
		{
			if (distCacheFiles [i].getName ().equals (dataFilename))
			{
				try
				{
					reader = new BufferedReader(new FileReader(distCacheFiles [i].toString ()));
					while ((line = reader.readLine()) != null)
					{
						myGIS.update (new Text (line), parcelColumnList);
						if (myGIS.attributes.get ("devtype").equals("C"))
						{
							Geometry geom = (Geometry) myGIS.geometry.clone ();
							C.put (new Integer (myGIS.attributes.get ("id")), geom);
						}
						else if (myGIS.attributes.get ("devtype").equals("I"))
						{
							Geometry geom = (Geometry) myGIS.geometry.clone ();
							I.put (new Integer (myGIS.attributes.get ("id")), geom);
						}
						else if (myGIS.attributes.get ("devtype").equals("G"))
						{
							Geometry geom = (Geometry) myGIS.geometry.clone ();
							G.put (new Integer (myGIS.attributes.get ("id")), geom);
						}
					}
				}
				catch (Exception e) { }

				break;
			}
		}
	}

	// For Mapper (via Closeable) interface
	public void close() {}

	// For Tool interface
	public int run(String[] args) throws Exception
	{
		JobConf job = new JobConf(new Configuration(), this.getClass());

		GISInputFormat.setInputPaths(job, new Path("/user/alaster/gis/jobs.gis"));
		GISOutputFormat.setOutputPath(job, new Path("output"));

		job.setJobName("test GIS - Large Jobs/Parcels");

		job.setMapperClass(this.getClass());
		//job.setCombinerClass(this.getClass());
		job.setReducerClass(this.getClass());
     
		job.setInputFormat(GISInputFormat.class);
		//job.setOutputFormat(TextOutputFormat.class);
		job.setOutputValueClass(LongWritable.class);

		Path p = new Path ("/user/alaster/gis/jobs.names");
		DistributedCache.addCacheFile (p.toUri (), job);
		job.set ("columnNames", p.getName ());

		p = new Path ("/user/alaster/gis/parcels.names");
		DistributedCache.addCacheFile (p.toUri (), job);
		job.set ("parcelColumnNames", p.getName ());

		p = new Path ("/user/alaster/gis/parcels.gis");
		DistributedCache.addCacheFile (p.toUri (), job);
		job.set ("parcelData", p.getName ());

		return JobClient.runJob(job).getJobState();
 	}

	// Hadoop runner requires this to be a static void!
	// Thus must use exit instead of return
	// Also must directly use the class name instead of figuring it out
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new TestGIS(), args));
	}
}
