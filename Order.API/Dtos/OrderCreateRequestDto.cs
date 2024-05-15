namespace Order.API.Dtos
{
    public record OrderCreateRequestDto(string UserId, decimal TotalPrice);
}