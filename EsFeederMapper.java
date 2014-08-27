package datacentermr;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		
		doc.put(new Text("hcan_SiteName"), new Text(line[1]));
		doc.put(new Text("hcan_RoomName"), new Text(line[2]));
		Date date = null;
		try {
			date = dateFormat.parse(line[3]);
			long time = date.getTime();
			doc.put(new Text("hcan_Fecha"), new FloatWritable(time));
			doc.put(new Text("hcan_Power"), new FloatWritable(Float.parseFloat(line[4])));
			doc.put(new Text("hcan_Temp"), new FloatWritable(Float.parseFloat(line[5])));
			doc.put(new Text("hcan_Humidity"), new FloatWritable(Float.parseFloat(line[6])));
			date = dateFormat.parse(line[7]);
			time = date.getTime();
			doc.put(new Text("hcan_Timestamp"), new FloatWritable(time));
			context.write(NullWritable.get(), doc);
		} catch (ParseException e) {
			log.error(e.getMessage());
		}
 }
}
