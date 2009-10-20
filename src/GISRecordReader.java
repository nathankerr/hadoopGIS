package hadoopGIS;

import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import hadoopGIS.GIS;
import java.io.IOException;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;

class GISRecordReader implements RecordReader<LongWritable, GIS>
{
	private List<String> columnList;
	private LineRecordReader reader;

	public GISRecordReader(Configuration job, FileSplit split) throws IOException
	{
		columnList = new ArrayList<String> ();
		reader = new LineRecordReader(job, (FileSplit) split);

		String columnFilename = job.get ("columnNames");
		Path[] distCacheFiles = new Path[0];
		try { distCacheFiles = DistributedCache.getLocalCacheFiles(job); }
		catch (IOException e)	{ return; }

		String line = new String ();
		for (int i=0; i<distCacheFiles.length; i++)
		{
			if (distCacheFiles [i].getName ().equals (columnFilename))
			{
				BufferedReader reader = new BufferedReader(new FileReader(distCacheFiles [i].toString ()));
				while ((line = reader.readLine()) != null)
					columnList.add (line);

				break;
			}
		}
	}

	public float getProgress()
	{
		return reader.getProgress();
	}

	public synchronized void close() throws IOException
	{
		reader.close();
	}

	public  synchronized long getPos() throws IOException
	{
		return reader.getPos();
	}

	public GIS createValue()
	{
		return new GIS();
	}

	public LongWritable createKey() {
		return new LongWritable();
	}

	public synchronized boolean next(LongWritable key, GIS value) throws IOException
	{
		Text textValue = new Text();
		if (reader.next(key, textValue)) {
			value.update(textValue, columnList);
			key.set(new Long(value.attributes.get("id")));
			return true;
		}
		return false;
	}
}
