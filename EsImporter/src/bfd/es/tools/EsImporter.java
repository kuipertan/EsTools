package bfd.es.tools;

import java.io.IOException;  
import java.util.StringTokenizer;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;  
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

  
public class EsImporter {  
	// esexporter output to hdfs format is : id        {_source}
	public static class EsImporterMapper extends Mapper<Object, Text, NullWritable, BytesWritable> {
        @Override       
        public void map(Object key, Text value, Mapper<Object, Text, NullWritable, BytesWritable>.Context context) throws IOException, InterruptedException {
            String tmp = value.toString().trim();
            int idx = tmp.indexOf("{\"");
            tmp = tmp.substring(idx);
        	byte[] source = tmp.getBytes();
            BytesWritable jsonDoc = new BytesWritable(source);
            context.write(NullWritable.get(), jsonDoc);
        }
    }
    
           
    public static void main(String[] args) throws Exception{   
    	Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();//获得输入参数 [hdfs://localhost:9000/user/dat/input, hdfs://localhost:9000/user/dat/output]    
        if(otherArgs.length != 3){//判断输入参数个数，不为两个异常退出 
        	//[es index type qstr]
            System.err.println("Usage: esexporter esip:port index  path");    
            System.exit(2);    
        } 
        
        conf.set("es.nodes", otherArgs[0]);    
        conf.set("es.resource", otherArgs[1]);            
        conf.set("es.input.json", "yes");   
        conf.set("es.resource.write",otherArgs[1]+"/{docType}");
        conf.set("es.mapping.id", "urlHash");
        conf.set("es.mapping.routing", "titleSimHash");

        Job job = Job.getInstance(conf,"hadoop es importer");
        //job.setJarByClass(EsExporter.class);
        job.setMapperClass(EsImporterMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        
        FileInputFormat.setInputPaths(job, new Path(otherArgs[2]));

        //job.waitForCompletion(true);
                        
        System.exit(job.waitForCompletion(true)?0:1);//是否正常退出    
    }   
}

