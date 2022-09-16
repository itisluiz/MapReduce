# TDE 2 - MapReduce

## Dados
| Index | Variable       | Description                                                   |
|-------|----------------|---------------------------------------------------------------|
| 0     | Country        | Country involved in the commercial transaction                |
| 1     | Year           | Year in which the transaction took place                      |
| 2     | Commodity code | Commodity identifier                                          |
| 3     | Commodity      | Commodity description                                         |
| 4     | Flow           | Flow, e.g. Export or Import                                   |
| 5     | Price          | Price, in USD                                                 |
| 6     | Weight         | Commodity weight                                              |
| 7     | Unit           | Unit in which the commodity is measured, e.g. Number of items |
| 8     | Amount         | Commodity amount given in the aforementioned unit             |
| 9     | Category       | Commodity category, e.g. Live animals                         |

Download [operacoes_comerciais_inteira.csv](https://drive.google.com/u/0/uc?id=1CK_LD7xX55J1C0Aqoilhoj1UkK06UBwQ&export=download)

## Objetivos
1. [x] The number of transactions involving Brazil;
2. [x] The number of transactions per year;
3. [x] The most commercialized commodity (summing the Amount column) in 2016, per flow type.
4. [ ] The average price of commodities per year;
5. [ ] The average price of commodities per unit type, year, and category in the export flow in Brazil;
6. [ ] The commodity with the highest price per unit type and year;
7. [ ] The number of transactions per flow type and year.