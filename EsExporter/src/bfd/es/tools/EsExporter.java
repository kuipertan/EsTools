package bfd.es.tools;

import java.io.IOException;  
import java.util.StringTokenizer;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;  
import org.elasticsearch.hadoop.mr.EsInputFormat;
  
public class EsExporter {  
    //嵌套类 Mapper    
    //Mapper<keyin,valuein,keyout,valueout>  
	/*
    public static class EsExporterMapper extends Mapper<Object, Text, Text, IntWritable>{    
        private final static IntWritable one = new IntWritable(1);    
        private Text word = new Text();    
            
        @Override    
        protected void map(Object key, Text value, Context context)    
                throws IOException, InterruptedException {    
            StringTokenizer itr = new StringTokenizer(value.toString());    
            while(itr.hasMoreTokens()){    
                word.set(itr.nextToken());    
                context.write(word, one);//Context机制    
            }    
        }    
    } 
    */
	/*
	public static class EsExporterMapper extends Mapper<Object, Object, Object, Object> {
		 @Override
		 protected void map(Object key, Object value, Context context)
		        throws IOException, InterruptedException {
		   Text docId = (Text) key;
		   MapWritable doc = (MapWritable) value;             
		   context.write(key, doc);
		 }
	}
	*/
	
	public static class EsExporterMapper extends Mapper<Writable, Writable, Text, Text> {
        @Override
        public void map(Writable key, Writable value, Mapper<Writable, Writable, Text, Text>.Context context)
                throws IOException, InterruptedException {
            context.write(new Text(key.toString()), new Text(value.toString()));
        }
    }
    
           
    public static void main(String[] args) throws Exception{   
    	Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();//获得输入参数 [hdfs://localhost:9000/user/dat/input, hdfs://localhost:9000/user/dat/output]    
        if(otherArgs.length != 4){//判断输入参数个数，不为两个异常退出 
        	//[es index type qstr]
            System.err.println("Usage: esexporter esip:port index  qstr path");    
            System.exit(2);    
        } 
        
        conf.set("es.output.json", "true");
        conf.set("es.nodes", otherArgs[0]);    
        conf.set("es.resource", otherArgs[1]);            
        conf.set("es.query", "?q=" + otherArgs[2]);    
        // "createTime:[1 TO 5]" 
        Job job = new Job(conf, "hadoop es");
        //job.setJarByClass(EsExporter.class);
        job.setMapperClass(EsExporterMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(EsInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);  
        
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        //job.waitForCompletion(true);
                        
        System.exit(job.waitForCompletion(true)?0:1);//是否正常退出    
    }   
}

