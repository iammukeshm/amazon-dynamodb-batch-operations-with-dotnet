using Amazon.DynamoDBv2.DataModel;

namespace DynamoBatch.Models;

[DynamoDBTable("products")]
public class Product
{
    [DynamoDBHashKey("id")]
    public Guid Id { get; set; }
    [DynamoDBProperty("name")]
    public string Name { get; set; } = default!;
    [DynamoDBProperty("description")]
    public string Description { get; set; } = default!;
    [DynamoDBProperty("price")]
    public decimal Price { get; set; }

    public Product()
    {

    }

    public Product(string name, string description, decimal price)
    {
        Id = Guid.NewGuid();
        Name = name;
        Description = description;
        Price = price;
    }
}