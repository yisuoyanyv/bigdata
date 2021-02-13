package gulivideo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class ETLDriver {
    public static void main(String[] args)  throws IOException,ClassNotFoundException,InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(ETLMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);//清洗数据不需要有Reducer

        FileInputFormat.setInputPaths((JobConf) job.getConfiguration(),args[0]);
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(),new Path(args[1]));

        job.waitForCompletion(true);

    }
}
