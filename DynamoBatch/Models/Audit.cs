using Amazon.DynamoDBv2.DataModel;

namespace DynamoBatch.Models;

[DynamoDBTable("audits")]
public class Audit
{
    [DynamoDBHashKey("id")]
    public Guid Id { get; set; }
    [DynamoDBRangeKey("product_id")]
    public Guid ProductId { get; set; }
    [DynamoDBProperty("action")]
    public string Action { get; set; } = default!;
    [DynamoDBProperty("time_stamp")]
    public DateTime TimeStamp { get; set; }
    public Audit()
    {

    }

    public Audit(Guid productId, string action)
    {
        Id = Guid.NewGuid();
        ProductId = productId;
        Action = action;
        TimeStamp = DateTime.UtcNow;
    }
}