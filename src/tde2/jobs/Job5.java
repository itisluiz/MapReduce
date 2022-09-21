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

import java.io.IOException;

// Objetivo: Obter a média dos preços das comodities por tipo de unidade, ano, categoria das transações exportadas do Brasil.
public class Job5
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();
        Job job = Job.getInstance(new Configuration(), "job5");

        if (!SetupHelper.setupIO(job, args))
            return;

        job.setJarByClass(Job5.class);
        SetupHelper.setupMapper(job, Map.class, CompositeKeyWritable.class, LongWritable.class);
        SetupHelper.setupReducer(job, Reduce.class, CompositeKeyWritable.class, DoubleWritable.class);

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, CompositeKeyWritable, LongWritable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            Transaction t = new Transaction(value.toString());

            if (key.get() == 0 && t.isHeader() || !t.isValid())
                return;

            if (!t.getFlow().equals("Export") || !t.getCountry().equals("Brazil"))
                return;

            con.write(new CompositeKeyWritable(String.valueOf(t.getYear()), t.getUnit(), t.getCategory()), new LongWritable(t.getPrice()));
        }
    }

    public static class Reduce extends Reducer<CompositeKeyWritable, LongWritable, CompositeKeyWritable, DoubleWritable>
    {
        public void reduce(CompositeKeyWritable key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException
        {
            double total = 0;
            long ocorrencias = 0;

            for (LongWritable value : values)
            {x''
                total += value.get();
                ++ocorrencias;
            }

            con.write(key, new DoubleWritable(total / ocorrencias));
        }
    }
}