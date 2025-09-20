using System.Text.Json;
using Common.Models;

namespace Common;

public class Validator
{
    public bool TryValidate(string json, out PurchaseEvent? purchaseEvent)
    {
        purchaseEvent = null;
        try
        {
            purchaseEvent = JsonSerializer.Deserialize<PurchaseEvent>(json);
            if (purchaseEvent == null) return false;

            if (string.IsNullOrWhiteSpace(purchaseEvent.UserId) ||
                string.IsNullOrWhiteSpace(purchaseEvent.Item))
            {
                return false;
            }
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
