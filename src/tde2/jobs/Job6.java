package tde2.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.BasicConfigurator;
import tde2.SetupHelper;
import tde2.Transaction;
import tde2.customwritable.CompositeKeyWritable;
import tde2.customwritable.KeyedLongWritable;

import java.io.IOException;
import java.util.*;

// Objetivo: Obter a commodity com o maior preço para cada par de tipo de unidade e ano

public class Job6
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();

        // Job A
        Job job_a = Job.getInstance(new Configuration(), "job6_a");

        if (!SetupHelper.setupIO(job_a, args))
            return;

        job_a.setJarByClass(Job6.class);
        SetupHelper.setupMapper(job_a, Map_a.class, CompositeKeyWritable.class, LongWritable.class);
        SetupHelper.setupReducer(job_a, Reduce_a.class, CompositeKeyWritable.class, LongWritable.class);

        job_a.waitForCompletion(true);

        // Job B
        Job job_b = Job.getInstance(new Configuration(), "job6_b");

        if (!SetupHelper.setupIO(job_b, args, false))
            return;

        job_b.setJarByClass(Job6.class);
        SetupHelper.setupMapper(job_b, Map_b.class, CompositeKeyWritable.class, KeyedLongWritable.class);
        SetupHelper.setupReducer(job_b, Reduce_b.class, CompositeKeyWritable.class, KeyedLongWritable.class);

        job_b.waitForCompletion(true);
    }

    // Job A
    public static class Map_a extends Mapper<LongWritable, Text, CompositeKeyWritable, LongWritable>
    {
        // Output: ano, tipo de unidade, commodity: preço transação
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            Transaction t = new Transaction(value.toString());

            if (key.get() == 0 && t.isHeader() || !t.isValid())
                return;

            con.write(new CompositeKeyWritable(String.valueOf(t.getYear()), t.getUnit(), t.getCommodity()), new LongWritable(t.getPrice()));
        }
    }

    public static class Reduce_a extends Reducer<CompositeKeyWritable, LongWritable, CompositeKeyWritable, LongWritable>
    {
        // Output: ano, tipo de unidade, commodity: preço total
        public void reduce(CompositeKeyWritable key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException
        {
            long total = 0;

            for (LongWritable value : values)
                total += value.get();

            con.write(key, new LongWritable(total));
        }
    }

    // Job B
    public static class Map_b extends Mapper<LongWritable, Text, CompositeKeyWritable, KeyedLongWritable>
    {
        // Output: ano, tipo de unidade: commodity, preço total
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            String[] components = value.toString().split("\t");

            CompositeKeyWritable yearUnitytype = new CompositeKeyWritable(components[0], components[1]);
            KeyedLongWritable commodityPrice = new KeyedLongWritable(components[2], Long.parseLong(components[3]));

            con.write(yearUnitytype, commodityPrice);
        }
    }

    public static class Reduce_b extends Reducer<CompositeKeyWritable, KeyedLongWritable, CompositeKeyWritable, KeyedLongWritable>
    {
        // Output: ano, tipo de unidade: commodity, preço total [Somente do maior preço total referente à chave]
        public void reduce(CompositeKeyWritable key, Iterable<KeyedLongWritable> values, Context con) throws IOException, InterruptedException
        {
            KeyedLongWritable max = null;
            for (KeyedLongWritable value : values)
            {
                if (max == null || max.getValue() < value.getValue())
                    max = value;
            }

            con.write(key, max);
        }
    }
}