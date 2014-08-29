package datacentermr;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class EsFeederMapper extends Mapper<Object, Text, NullWritable, MapWritable> {
	private static Logger log = Logger.getLogger(EsFeederMapper.class);
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
	 
		MapWritable doc = new MapWritable();
		String[] line = value.toString().split(",");
		
		doc.put(new Text("hcan_SiteName"), new Text(line[1]));
		doc.put(new Text("hcan_RoomName"), new Text(line[2]));
		doc.put(new Text("hcan_Fecha"), new Text(line[3].replace(' ','T')));
		doc.put(new Text("hcan_Power"), new FloatWritable(Float.parseFloat(line[4])));
		doc.put(new Text("hcan_Temp"), new FloatWritable(Float.parseFloat(line[5])));
		doc.put(new Text("hcan_Humidity"), new FloatWritable(Float.parseFloat(line[6])));
		doc.put(new Text("hcan_Timestamp"), new Text(line[6].replace(' ','T')));
		context.write(NullWritable.get(), doc);
 }
}
