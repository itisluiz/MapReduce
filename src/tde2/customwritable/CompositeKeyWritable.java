package tde2.customwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable>
{
    String[] compoenents;

    public CompositeKeyWritable() { }

    public String getComponent(int index)
    {
        return compoenents[index];
    }

    public CompositeKeyWritable(String... strings)
    {
        compoenents = strings;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(compoenents);
    }

    @Override
    public int compareTo(CompositeKeyWritable o)
    {
        return this.hashCode() - o.hashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(compoenents.length);

        for (String s : compoenents)
            Text.writeString(dataOutput, s);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        int length = dataInput.readInt();
        compoenents = new String[length];

        for (int i = 0; i < length; ++i)
            compoenents[i] = Text.readString(dataInput);
    }

    @Override
    public String toString()
    {
        return String.join("\t", compoenents);
    }

}
