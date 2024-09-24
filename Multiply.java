import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.util.Hashtable;
import java.util.Enumeration;

class Elem implements Writable{
    String matrix;
    int index;
    double value;


Elem (){
    this.matrix = "M";
    this.index = 0;
    this.value = 0.0;
}

Elem (String matrix, int index, double value){

    this.matrix = matrix;
    this.index = index;
    this.value = value;
}

@Override
	public void readFields(DataInput in) throws IOException {
		matrix = in.readUTF();
		index = in.readInt();
		value = in.readDouble();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(matrix);
		out.writeInt(index);
		out.writeDouble(value);
	}
}



public class Multiply {
    public static class First_Mapper extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String s = value.toString();
            String parts[] = s.split(",");
            Integer i = Integer.parseInt(parts[1]);
            Integer index = Integer.parseInt(parts[0]);
            double val = Double.parseDouble(parts[2]);
            Elem e = new Elem("M", index, val);

            context.write(new IntWritable(i),e);
            
        }
    }

    public static class Second_Mapper extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String s = value.toString();
            String parts[] = s.split(",");
            Integer i = Integer.parseInt(parts[0]);
            Integer index = Integer.parseInt(parts[1]);
            double val = Double.parseDouble(parts[2]);
            Elem e = new Elem("N", index, val);
            context.write(new IntWritable(i),e);
           
        }
    }

    public static class MyReducer extends Reducer<IntWritable,Elem,Text,Text> {
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {
            String s;
            int[] FirstMatrix = new int[100000];
            double[] FirstValues = new double[100000];
            int[] SecondMatrix = new int[100000];
            double[] SecondValues = new double[100000];
            int i = -1, j = -1;
            for (Elem v: values) {
                //System.out.println("key " + key + " "+ v.matrix+ " "+v.index+" "+v.value);   
                
                if(v.matrix.equals("M")){
                  
                   i = i + 1;
                   FirstMatrix[i] = v.index;
                   FirstValues[i] = v.value;
                }
                else{
                    j = j + 1;
                    SecondMatrix[j] = v.index;
                    SecondValues[j] = v.value;
                }
            }
             
            Hashtable<String, Double> hashtable = new Hashtable<>();
            for(int k = 0; k<=i; k++){
                for(int l = 0; l<=j; l++){
                  
                    double  product = FirstValues[k]*SecondValues[l];
                   
                    String hash_key = Integer.toString(FirstMatrix[k]) + "," + Integer.toString(SecondMatrix[l]);
                   
                    Double  value = hashtable.get(hash_key);
                    if(value == null){
                        hashtable.put(hash_key, product);
                    }
                    else{
                        value = value + product;
                        hashtable.put(hash_key, value);
                    }
                }
            }
            Enumeration<String> keys = hashtable.keys();
            while (keys.hasMoreElements()) {
                String each_key = keys.nextElement();
                Double each_value = hashtable.get(each_key);
                String out = each_value.toString();
                context.write(new Text(each_key), new Text(out));
                }
        
        }
    }

    public static class Third_Mapper extends Mapper<Object,Text,Text,Text> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String s = value.toString();
            String parts[] = s.split(",");
            String k = parts[0] + "," + parts[1];
            context.write(new Text(k), new Text(parts[2]));
        }
    }

    public static class Second_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double total = 0;
            for (Text v : values) {
                String stringValue = v.toString().trim(); // Convert Text to String
                double parsedValue = Double.parseDouble(stringValue);
                total += parsedValue;
            }
            String out = String.valueOf(total).trim(); 
            String k = key.toString().trim();
            context.write(new Text(k), new Text(out));
        }

    }    


    public static void main ( String[] args ) throws Exception {
        Configuration conf = new Configuration();

        conf.set("mapreduce.output.textoutputformat.separator", ",");
        //conf.set("mapreduce.input.fileinputformat.split.maxsize", "16777216");

        long startTimeJob1 = System.currentTimeMillis();
       
        Job job = Job.getInstance(conf, "FirstJob");
        job.setJarByClass(Multiply.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Elem.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,First_Mapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Second_Mapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        //job.setNumReduceTasks(5);
        job.waitForCompletion(true);
       
 

        long endTimeJob1 = System.currentTimeMillis();
        long durationJob1 = endTimeJob1 - startTimeJob1;
        System.out.println("Job 1 Execution Time: " + durationJob1 + " milliseconds");

        long startTimeJob2 = System.currentTimeMillis();

        Job job2 = Job.getInstance(conf, "SecondJob");
		job2.setJarByClass(Multiply.class);
		job2.setMapperClass(Third_Mapper.class);
		job2.setReducerClass(Second_Reducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        //job2.setNumReduceTasks(5);
		job2.waitForCompletion(true);

        long endTimeJob2 = System.currentTimeMillis();
        long durationJob2 = endTimeJob2 - startTimeJob2;
        
        System.out.println("Job 2 Execution Time: " + durationJob2 + " milliseconds");

        long totalDuration = durationJob1 + durationJob2;
        System.out.println("Total Job Execution Time for 16MB and 5 reduce tasks: " + totalDuration + " milliseconds");
    }
}
