import org.apache.hadoop.io.Text;

public class Transaction
{
    private final String[] components;

    Transaction(String transaction)
    {
        components = transaction.split(";");
    }

    public String getCountry()
    {
        return components[0];
    }

    public String getYear()
    {
        return components[1];
    }

    public String getCommodityCode()
    {
        return components[2];
    }

    public String getCommodity()
    {
        return components[3];
    }

    public String getFlow()
    {
        return components[4];
    }

    public String getPrice()
    {
        return components[5];
    }

    public String getWeight()
    {
        return components[6];
    }

    public String getUnit()
    {
        return components[7];
    }

    public String getAmount()
    {
        return components[8];
    }

    public String getCategory()
    {
        return components[9];
    }

}
