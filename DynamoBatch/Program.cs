using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoBatch.DTOs;
using DynamoBatch.Models;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddAWSService<IAmazonDynamoDB>();
builder.Services.AddScoped<IDynamoDBContext, DynamoDBContext>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapPost("/products/batch-write", async (List<CreateProductDto> productDtos, IDynamoDBContext context) =>
{
    var products = new List<Product>();
    foreach (var product in productDtos)
    {
        products.Add(new Product(product.Name, product.Description, product.Price));
    }
    var batchWrite = context.CreateBatchWrite<Product>();
    batchWrite.AddPutItems(products);
    await batchWrite.ExecuteAsync();
});

app.MapPost("/products/batch-write-with-audits", async (List<CreateProductDto> productDtos, IDynamoDBContext context) =>
{
    var products = new List<Product>();
    foreach (var product in productDtos)
    {
        products.Add(new Product(product.Name, product.Description, product.Price));
    }
    var batchProductWrite = context.CreateBatchWrite<Product>();
    batchProductWrite.AddPutItems(products);

    var audits = new List<Audit>();
    foreach (var product in products)
    {
        audits.Add(new Audit(product.Id, "create"));
    }
    var batchAuditWrite = context.CreateBatchWrite<Audit>();
    batchAuditWrite.AddPutItems(audits);
    var batchWrites = batchProductWrite.Combine(batchAuditWrite);

    await batchWrites.ExecuteAsync();
});

app.MapPost("/products/batch-get", async (IDynamoDBContext context, [FromBody] List<Guid> productIds) =>
{
    var batchGet = context.CreateBatchGet<Product>();
    foreach (var id in productIds)
    {
        batchGet.AddKey(id);
    }

    await batchGet.ExecuteAsync();
    return Results.Ok(batchGet.Results);
});

app.MapPost("/products/batch-delete", async (IDynamoDBContext context, [FromBody] List<Guid> productIds) =>
{
    var batchDelete = context.CreateBatchWrite<Product>();
    foreach (var id in productIds)
    {
        batchDelete.AddDeleteKey(id);
    }
    await batchDelete.ExecuteAsync();
});


app.MapPost("/products/fail-safe-batch-write", async (List<CreateProductDto> productDtos, IAmazonDynamoDB context) =>
{
    var products = new List<Product>();
    foreach (var product in productDtos)
    {
        products.Add(new Product(product.Name, product.Description, product.Price));
    }

    var request = new BatchWriteItemRequest
    {
        RequestItems = new Dictionary<string, List<WriteRequest>>
        {
            {
                "products",
                products.Select(p => new WriteRequest(
                    new PutRequest(new Dictionary<string, AttributeValue>
                    {
                        { "id", new AttributeValue { S = p.Id.ToString() } },
                        { "name", new AttributeValue { S = p.Name } },
                        { "description", new AttributeValue { S = p.Description} },
                        { "price", new AttributeValue { N = $"{p.Price}"} },
                    })))
                .ToList()
            }
        }
    };

    var maxRetries = 5;
    var delay = 200; // Initial delay of 200ms

    async Task RetryBatchWriteAsync(Dictionary<string, List<WriteRequest>> unprocessedItems)
    {
        var retryCount = 0;
        while (retryCount < maxRetries && unprocessedItems.Count > 0)
        {
            var retryRequest = new BatchWriteItemRequest
            {
                RequestItems = unprocessedItems
            };

            var retryResponse = await context.BatchWriteItemAsync(retryRequest);

            // Check if there are still unprocessed items
            unprocessedItems = retryResponse.UnprocessedItems;

            if (unprocessedItems.Count == 0)
            {
                return; // Exit if no unprocessed items remain
            }

            // Apply exponential backoff
            await Task.Delay(delay);
            delay *= 2; // Double the delay for each retry
            retryCount++;
        }

        if (unprocessedItems.Count > 0)
        {
            throw new Exception("Max retry attempts exceeded. Some items were not processed.");
        }
    }

    var response = await context.BatchWriteItemAsync(request);

    if (response != null && response.UnprocessedItems.Count > 0)
    {
        // Retry unprocessed items with exponential backoff
        await RetryBatchWriteAsync(response.UnprocessedItems);
    }

    return Results.Ok("Batch write operation completed.");
});


app.UseHttpsRedirection();
app.Run();