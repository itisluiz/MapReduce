package tde2.customwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class KeyedLongWritable implements WritableComparable<KeyedLongWritable>
{
    String key;
    long value;

    public String getKey()
    {
        return key;
    }

    public void setKey(String key)
    {
        this.key = key;
    }

    public long getValue()
    {
        return value;
    }

    public void setValue(long value)
    {
        this.value = value;
    }

    public KeyedLongWritable() { }
    public KeyedLongWritable(String key, long value)
    {
        this.key = key;
        this.value = value;
    }

    @Override
    public int hashCode()
    {
        return key.hashCode() + Long.hashCode(value);
    }

    @Override
    public int compareTo(KeyedLongWritable o)
    {
        return this.hashCode() - o.hashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        Text.writeString(dataOutput, key);
        dataOutput.writeLong(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        key = Text.readString(dataInput);
        value = dataInput.readLong();
    }

    @Override
    public String toString()
    {
        return key + "\t" + value;
    }

}
