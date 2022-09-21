package tde2.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.BasicConfigurator;
import tde2.SetupHelper;
import tde2.Transaction;

import java.io.IOException;

// Objetivo: Obter a relação de número de transações envolvendo cada país.
public class Job1
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();
        Job job = Job.getInstance(new Configuration(), "job1");

        if (!SetupHelper.setupIO(job, args))
            return;

        job.setJarByClass(Job1.class);
        SetupHelper.setupMapper(job, Map.class, Text.class, IntWritable.class);
        SetupHelper.setupReducer(job, Reduce.class, Text.class, IntWritable.class);

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            Transaction t = new Transaction(value.toString());

            if (key.get() == 0 && t.isHeader() || !t.isValid())
                return;

            con.write(new Text(t.getCountry()), new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
        {
            int total = 0;

            if (String.valueOf(key).equals("Brazil")) {
                for (IntWritable value : values)
                    total += value.get();

                con.write(key, new IntWritable(total));
            }

        }
    }
}