/* =============================================================================
   Generate realistic OLTP changes so CDC / SCD2 / watermark loads have
   something to pick up. Run repeatedly (manually, or on a schedule) during demos.
============================================================================= */

SET NOCOUNT ON;

-- 1) SCD2 demo: customers move city / get a new credit limit
UPDATE c
   SET DeliveryCityID = (SELECT TOP 1 CityID FROM Application.Cities ORDER BY NEWID()),
       CreditLimit    = ISNULL(CreditLimit, 1000) * 1.10
FROM Sales.Customers c
WHERE c.CustomerID IN (SELECT TOP 5 CustomerID FROM Sales.Customers ORDER BY NEWID());

-- 2) SCD2 demo: price change on a few products
UPDATE si
   SET UnitPrice = ROUND(UnitPrice * (0.9 + RAND(CHECKSUM(NEWID())) * 0.2), 2)
FROM Warehouse.StockItems si
WHERE si.StockItemID IN (SELECT TOP 3 StockItemID FROM Warehouse.StockItems ORDER BY NEWID());

-- 3) SCD1 merge demo: edit recent orders and order lines
UPDATE o
   SET Comments = CONCAT('edited ', FORMAT(SYSUTCDATETIME(), 'yyyy-MM-dd HH:mm:ss')),
       LastEditedWhen = SYSDATETIME()
FROM Sales.Orders o
WHERE o.OrderID IN (SELECT TOP 20 OrderID FROM Sales.Orders ORDER BY OrderID DESC);

UPDATE ol
   SET Quantity = Quantity + 1,
       LastEditedWhen = SYSDATETIME()
FROM Sales.OrderLines ol
WHERE ol.OrderLineID IN (SELECT TOP 20 OrderLineID FROM Sales.OrderLines ORDER BY NEWID());

-- 4) Watermark demo: stock movements bump LastEditedWhen
UPDATE h
   SET QuantityOnHand = CASE WHEN QuantityOnHand > 50 THEN QuantityOnHand - 25 ELSE QuantityOnHand + 500 END,
       LastEditedWhen = SYSDATETIME()
FROM Warehouse.StockItemHoldings h
WHERE h.StockItemID IN (SELECT TOP 15 StockItemID FROM Warehouse.StockItemHoldings ORDER BY NEWID());

PRINT 'Changes applied at ' + CONVERT(VARCHAR(30), SYSUTCDATETIME(), 126);
