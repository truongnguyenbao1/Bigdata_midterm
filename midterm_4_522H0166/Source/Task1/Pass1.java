import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pass1 {
    // Mapper: Đếm số lần xuất hiện của mỗi khách hàng
    public static class CustomerMapper 
        extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text customerID = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Bỏ qua header
            if (key.get() == 0) return;
            
            String[] fields = value.toString().split(",");
            customerID.set(fields[0]); // Member_number
            context.write(customerID, one);
        }
    }

 // Reducer: Tổng hợp và lọc theo ngưỡng min_support
    public static class CustomerReducer 
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private static final int MIN_SUPPORT = 3;
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            if (sum >= MIN_SUPPORT) {
                result.set(sum);
                context.write(key, result);
            }
        }
    }
public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "First Pass - Customer Count");
        
        job.setJarByClass(FirstPass.class);
        job.setMapperClass(CustomerMapper.class);
        job.setReducerClass(CustomerReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
