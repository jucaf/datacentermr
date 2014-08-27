package datacentermr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.elasticsearch.hadoop.mr.EsOutputFormat;


public class EsFeeder extends Configured implements Tool  {
	
	
	private static Logger log = Logger.getLogger(EsFeeder.class);
	private String servers = "localhost:9200";
	private String index = "default";
	private String input;
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new EsFeeder(), args);
		System.exit(res);
	}
	
	public void optParser(String[] args){
		int i = 0;
		while (i < args.length ) {
			switch(args[i]){
			case "--servers": case "-s":
				if ((args.length > i+1) && (!args[i+1].startsWith("-"))){
					i++;
					this.servers = args[i];
				} else {
					log.error("server option requires at least one server");
					log.error("Usage: [--servers|-s [server:port]+] [--index index] --input|-i input_path");
					System.exit(-1);
				}
				break;
			case "--index":
				if ((args.length > i+1) && (!args[i+1].startsWith("-"))){
					i++;
					this.index = args[i];
				} else {
					log.error("index option requires at least index");
					log.error("Usage: [--servers|-s [server:port]+] [--index index] --input|-i input_path");
					System.exit(-1);
				}				
				break;
			case "--input":case "-i":
				if ((args.length > i+1) && (!args[i+1].startsWith("-"))){
					i++;
					this.input = args[i]; 
				} else {
					log.error("input option requires a path");
					log.error("Usage: [--servers|-s [server:port]+] [--index index] --input|-i input_path");
					System.exit(-1);
				}
				break;				
			default:
				log.error("Unrecognized option: " + args[i] ) ;
				break;
			}
			i++;
		}
		if ( this.input == null || this.input.isEmpty() ){
			log.error("input option is required");
			log.error("Usage: [--servers|-s [server:port]+] [--index index] --input|-i input_path");
			System.exit(-1);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception{
		Configuration conf = super.getConf();
		optParser(args);
				
		conf.set("es.nodes", this.servers);
		conf.set("es.resource", this.index + "/{hcan_SiteName}");
		
		Job job = Job.getInstance(conf,"Description");
		job.setJarByClass(EsFeeder.class);
		job.setMapperClass(datacentermr.EsFeederMapper.class);
		job.setSpeculativeExecution(false);
		
		job.setOutputFormatClass(EsOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(this.input));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
		}
}
