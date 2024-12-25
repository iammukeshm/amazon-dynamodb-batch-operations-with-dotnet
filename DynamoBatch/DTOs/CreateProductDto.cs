namespace DynamoBatch.DTOs;

public class CreateProductDto
{
    public string Name { get; set; } = default!;
    public string Description { get; set; } = default!;
    public decimal Price { get; set; }
}
