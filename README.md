# US_Stock
This project is to find out **potential stocks**.
The rules are adopted from [Trade Like a Stock Market Wizard](https://www.amazon.com/Trade-Like-Stock-Market-Wizard/dp/0071807225).

## Prerequisites
1. Backwardly retrieve at least 1-year stock indices through **def renew_index_volume of StockUtil**
2. Compute 50MA, 150MA, 200MA, etc.
3. Store data into database

## Code Flow
1. Read a csv of stock names
2. Fetch yesterday's index of each stock
3. Compute 50MA, 150MA, 200MA, etc.
4. Store data into database
5. Use rules to find out potential stocks
5. Employee **Line Notify** to send potential stocks to my phone [![](https://imgur.com/PckpCSd.png)]()
