package hadoopGIS;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.util.ArrayList;
import org.apache.hadoop.io.BinaryComparable;
import com.vividsolutions.jts.geom.Coordinate;
import java.io.DataInput;
import java.io.DataOutput;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import java.util.HashMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

public class GIS extends BinaryComparable implements Writable
{

	public Geometry geometry;
	public HashMap<String, String> attributes;
	private List<String> columns;

	public GIS() {
		geometry = new GeometryFactory().createPoint(new Coordinate(0,0));
		attributes = new HashMap<String, String>(32);
		columns = (List<String>) new ArrayList<String>();
	}

	public Text toText()
	{
		return new Text(hashToString(attributes) + "\n");
	}

	// For BinaryComparable
	public byte[] getBytes()
	{
		return toText().getBytes();
	}

	public int getLength()
	{
		return toText().getLength();
	}

	public boolean update(Text value, List<String> columnList)
	{
		columns.clear ();
		attributes.clear ();
		String[] splits = value.toString().split("\",\"");

		columns.addAll (columnList);
		for (int i=0; i < splits.length; i++)
		{
			// Erase begining and ending quotes/commas
			splits[i] = splits[i].replaceAll("^\"","");
			splits[i] = splits[i].replaceAll("\"$","");

			attributes.put(columns.get(i), splits[i]);

			if (columnList.size () == 0)
				columns.add (String.valueOf (i));
		}

		try {
			geometry = new WKTReader().read( new String ((String) attributes.get("the_geom")));
		}
		catch (com.vividsolutions.jts.io.ParseException e) { }

		return true;
	}

	public String toString()
	{
		StringBuilder finalString = new StringBuilder ();
		for (int i=0; i<columns.size (); i++)
		{
			finalString.append ("\"");
			finalString.append (attributes.get (columns.get (i)));
			finalString.append ("\"");
			if (i != columns.size ()-1)
				finalString.append(",");
		}

		finalString.append ("\n");
		return finalString.toString ();
	}

	public void write(DataOutput out) throws IOException {
		Text value = toText();
		value.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		Text value = new Text();
		value.readFields(in);
		attributes.putAll (stringToHash (value.toString()));
		try {
			geometry = new WKTReader().read( new String ((String) attributes.get("the_geom")));
		}
		catch (com.vividsolutions.jts.io.ParseException e) { }
	}

	// Outputs "key1"="value1","key2"="value2" after escaping \",= characters
	private String hashToString(HashMap<String,String> h)
	{
		h.put ("the_geom", geometry.toText ());
		String key, value, finalString = "";

		for (int i=0; i<columns.size (); i++)
		{
			// Escape string
			key = columns.get (i);
			value = attributes.get(key);

			key = key.replace("^\"","");
			value = value.replace("\"$","");

			key = key.replace("\\", "\\\\");
			key = key.replace("\"", "\\\"");
			key = key.replace("=", "\\=");
			key = key.replace(",", "\\,");

			value = value.replace("\\", "\\\\");
			value = value.replace("\"", "\\\"");
			value = value.replace("=", "\\=");
			value = value.replace(",", "\\,");

			finalString += "\"" + key + "\"=\"" + value + "\"";
			if(i != columns.size ()-1)
				finalString += ",";
		}

		return finalString;
	}

	private HashMap<String,String> stringToHash(String str)
	{
		if(str == null)
			return null;

		columns.clear ();
		str = str.substring (0, str.length ()-1);
		HashMap<String,String> h = new HashMap<String,String>();
		String [] splits = str.split("\",\"");
		for(int i=0; i<splits.length; i++)
		{
			String [] pair = splits [i].split("\"=\"");
			if(pair.length != 2)
			{
				h.put (pair [0], "");
				columns.add (pair [0]);
				continue;
			}

			// Unescape string
			pair[0] = pair[0].replace("\\\\", "\\");
			pair[0] = pair[0].replace("\\\"", "\"");
			pair[0] = pair[0].replace("\\=", "=");
			pair[0] = pair[0].replace("\\,", ",");

			pair[1] = pair[1].replace("\\\\", "\\");
			pair[1] = pair[1].replace("\\\"", "\"");
			pair[1] = pair[1].replace("\\=", "=");
			pair[1] = pair[1].replace("\\,", ",");

			pair[1] = pair[1].replace("\"","");
			pair[0] = pair[0].replace("\"","");

			h.put(pair [0], pair [1]);
			columns.add (pair [0]);
		}

		return h;
	}
}
