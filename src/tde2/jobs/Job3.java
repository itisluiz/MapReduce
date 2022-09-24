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
import tde2.customwritable.CompositeKeyWritable;
import tde2.customwritable.Job3Writable;

import java.io.IOException;

// The most commercialized commodity (summing the Amount column) in 2016, per flow type.
// Objetivo: Obter a relação de commodities mais comercializados em 2016, separados por Flow
public class Job3
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();

        // Primeira rotina MapReduce
        Job job_a = Job.getInstance(new Configuration(), "job3_a");

        if (!SetupHelper.setupIO(job_a, args))
            return;

        job_a.setJarByClass(Job3.class);
        SetupHelper.setupMapper(job_a, Map.class, CompositeKeyWritable.class, LongWritable.class);
        SetupHelper.setupReducer(job_a, Reduce.class, CompositeKeyWritable.class, LongWritable.class);

        job_a.waitForCompletion(true);

        // Segunda rotina MapReduce
        Job job_b = Job.getInstance(new Configuration(), "job3_b");

        if (!SetupHelper.setupIO(job_b, args, false))
            return;

        job_b.setJarByClass(Job3.class);
        SetupHelper.setupMapper(job_b, Map2.class, Text.class, Job3Writable.class);
        SetupHelper.setupReducer(job_b, Reduce2.class, Text.class, Job3Writable.class);

        job_b.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, CompositeKeyWritable, LongWritable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            Transaction t = new Transaction(value.toString());

            if (key.get() == 0 && t.isHeader() || !t.isValid())
                return;

            if (t.getYear() != 2016)
                return;

            //con.write(new Text(t.getCommodity() + "\t" + t.getFlow()), new LongWritable(t.getAmount()));
            con.write(new CompositeKeyWritable(t.getFlow(), t.getCommodity()), new LongWritable(t.getAmount()));
        }
    }

    public static class Reduce extends Reducer<CompositeKeyWritable, LongWritable, CompositeKeyWritable, LongWritable>
    {
        public void reduce(CompositeKeyWritable key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException
        {
            long total = 0;

            for (LongWritable value : values)
                total += value.get();

            con.write(key, new LongWritable(total));
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, Job3Writable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            String linha = value.toString();
            String[] linhas = linha.split("\t");

            String flow = linhas[0];
            String commodity = linhas[1];
            long soma = Long.parseLong(linhas[2]);

            con.write(new Text(flow), new Job3Writable(commodity,soma));
        }
    }

    public static class Reduce2 extends Reducer<Text, Job3Writable, Text, Job3Writable>
    {
        public void reduce(Text key, Iterable<Job3Writable> values, Context con) throws IOException, InterruptedException
        {
            Job3Writable max = null;

            for (Job3Writable value : values)
                if (max == null || value.getSoma() > max.getSoma())
                    max = value;

            con.write(key,max);
        }
    }



}