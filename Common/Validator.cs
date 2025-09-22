using System.Text.Json;
using Common.Models;

namespace Common;

public class Validator
{
    //makes sure incoming messages are valid PurchaseEvent objects before processing
    //takes json, if purchase event is valid, returns true and outputs the deserialized object, otherwise outputs null
    public bool TryValidate(string json, out PurchaseEvent? purchaseEvent) 
    {
        purchaseEvent = null; //starts null to avoid uninitialized variable issues
        try
        {
            purchaseEvent = JsonSerializer.Deserialize<PurchaseEvent>(json); //converts json to string(purchaseevent)
            if (purchaseEvent == null) return false;

            if (string.IsNullOrWhiteSpace(purchaseEvent.UserId) ||
                string.IsNullOrWhiteSpace(purchaseEvent.Item))
            {
                return false;
            }
            return true;
        }
        catch (JsonException) //error handling for invalid json
        {
            return false;
        }
    }
}
