import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pass2 {
    public class Constants {
        // File paths
        public static final String OUTPUT1_PATH = "/user/hadoop/output1/part-r-00000";
        public static final String HEADER_LINE = "Member_number";
        
        // Support thresholds
        public static final int MIN_SUPPORT = 10;
        public static final int MIN_PAIR_SUPPORT = 5;
        
        // Delimiters
        public static final String TAB_DELIMITER = "\t";
        public static final String COMMA_DELIMITER = ",";
        
        // Job configurations
        public static final String JOB_NAME = "Pass2 - Frequent Customer Pairs";
    }

    public static class PairMapper 
        extends Mapper<LongWritable, Text, Text, Text> {
        
        private Set<String> frequentCustomers = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            Path output1Path = new Path(Constants.OUTPUT1_PATH);
            FileSystem fs = FileSystem.get(conf);
            
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(output1Path)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(Constants.TAB_DELIMITER);
                    if (parts.length == 2) {
                        String customerID = parts[0].trim();
                        int frequency = Integer.parseInt(parts[1].trim());
                        if (frequency >= Constants.MIN_SUPPORT) {
                            frequentCustomers.add(customerID);
                        }
                    }
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.startsWith(Constants.HEADER_LINE)) return;
            
            String[] fields = line.split(Constants.COMMA_DELIMITER);
            if (fields.length >= 2) {
                String customerID = fields[0].trim();
                String date = fields[1].trim();
                
                if (frequentCustomers.contains(customerID)) {
                    context.write(new Text(date), new Text(customerID));
                }
            }
        }
    }

    public static class PairReducer 
        extends Reducer<Text, Text, Text, IntWritable> {
        
        private Map<String, Integer> pairCounts = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> customersInDay = new ArrayList<>();
            for (Text val : values) {
                customersInDay.add(val.toString());
            }
            
            for (int i = 0; i < customersInDay.size() - 1; i++) {
                for (int j = i + 1; j < customersInDay.size(); j++) {
                    String customer1 = customersInDay.get(i);
                    String customer2 = customersInDay.get(j);
                    String pair = customer1.compareTo(customer2) < 0 ? 
                                customer1 + Constants.COMMA_DELIMITER + customer2 : 
                                customer2 + Constants.COMMA_DELIMITER + customer1;
                    
                    pairCounts.merge(pair, 1, Integer::sum);
                }
            }
        }

        @Override
        protected void cleanup(Context context) 
                throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : pairCounts.entrySet()) {
                if (entry.getValue() >= Constants.MIN_PAIR_SUPPORT) {
                    context.write(
                        new Text(entry.getKey()), 
                        new IntWritable(entry.getValue())
                    );
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Constants.JOB_NAME);
        
        job.setJarByClass(SecondPass.class);
        job.setMapperClass(PairMapper.class);
        job.setReducerClass(PairReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
