package datacentermr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EsFeederMapper extends Mapper<Object, Text, NullWritable, MapWritable> {
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String prefix = conf.get("prefix");
		
		MapWritable doc = new MapWritable();
		String[] line = value.toString().split(",");
		doc.put(new Text(prefix+"Id"),new Text(line[1]+"-"+line[2]+"-"+line[0]));
		doc.put(new Text(prefix+"SiteName"), new Text(line[1]));
		doc.put(new Text(prefix+"RoomName"), new Text(line[2]));
		doc.put(new Text(prefix+"Fecha"), new Text(line[3].replace(' ','T')));
		doc.put(new Text(prefix+"Power"), new FloatWritable(Float.parseFloat(line[4])));
		doc.put(new Text(prefix+"Temp"), new FloatWritable(Float.parseFloat(line[5])));
		doc.put(new Text(prefix+"Humidity"), new FloatWritable(Float.parseFloat(line[6])));
		doc.put(new Text(prefix+"Timestamp"), new Text(line[6].replace(' ','T')));
		
		context.write(NullWritable.get(), doc);
 }
}
