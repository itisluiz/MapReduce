package tde2;

public class Transaction
{
    private final String[] components;

    private long parseLongOrZero(String component)
    {
        try
        {
            return Long.parseLong(component);
        }
        catch (NumberFormatException e)
        {
            return 0l;
        }
    }

    public Transaction(String transaction)
    {
        components = transaction.split(";");
    }

    public boolean isHeader()
    {
        return components[0].equals("country_or_area");
    }

    public boolean isValid()
    {
        return components.length == 10;
    }

    public String getCountry()
    {
        return components[0];
    }

    public long getYear()
    {
        return parseLongOrZero(components[1]);
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

    public long getPrice()
    {
        return parseLongOrZero(components[5]);
    }

    public long getWeight()
    {
        return parseLongOrZero(components[6]);
    }

    public String getUnit()
    {
        return components[7];
    }

    public long getAmount()
    {
        return parseLongOrZero(components[8]);
    }

    public String getCategory()
    {
        return components[9];
    }

}
