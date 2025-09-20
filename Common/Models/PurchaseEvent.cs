namespace Common.Models;

public class PurchaseEvent
{
    public string UserId { get; set; } = "";
    public string Item { get; set; } = "";
    public DateTime Timestamp { get; set; }
}
