package tde2.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.BasicConfigurator;
import tde2.SetupHelper;
import tde2.Transaction;

import java.io.IOException;

// Objetivo: Obter a relação de commodities mais comercializados em 2016, separados por Flow
public class Job3
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();
        Job job = Job.getInstance(new Configuration(), "job3");

        if (!SetupHelper.setupIO(job, args))
            return;

        job.setJarByClass(Job3.class);
        SetupHelper.setupMapper(job, Map.class, Text.class, LongWritable.class);
        SetupHelper.setupReducer(job, Reduce.class, Text.class, LongWritable.class);

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            Transaction t = new Transaction(value.toString());

            if (key.get() == 0 && t.isHeader() || !t.isValid())
                return;

            if (t.getYear() != 2016)
                return;

            con.write(new Text(t.getCommodity() + "\t" + t.getFlow()), new LongWritable(t.getAmount()));
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException
        {
            long total = 0;

            for (LongWritable value : values)
                total += value.get();

            con.write(key, new LongWritable(total));
        }
    }
}