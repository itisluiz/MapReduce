package tde2.customwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Job3Writable implements WritableComparable<Job3Writable>
{
    String commodity;
    long soma;

    public Job3Writable() { }

    public Job3Writable(String commodity, long soma) {
        this.commodity = commodity;
        this.soma = soma;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public long getSoma() {
        return soma;
    }

    public void setSoma(long soma) {
        this.soma = soma;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return commodity + "\t" + soma;
    }

    @Override
    public int compareTo(Job3Writable o) {
        if (this.hashCode() < o.hashCode()) {
            return -1;
        }
        else if (this.hashCode() > o.hashCode()) {
            return +1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity);
        dataOutput.writeLong(soma);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        soma = dataInput.readLong();
    }
}
