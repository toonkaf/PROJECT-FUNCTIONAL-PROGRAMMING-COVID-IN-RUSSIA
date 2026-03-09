# High-Performance Data Pipeline
# COVID-19 in Russia Data Analysis using Scala

This project analyzes COVID-19 data from a CSV dataset using Scala functional programming and parallel processing techniques.

## Features
- Total Impact Calculation
- Daily Summary Aggregation
- Daily City Summary
- Sequential vs Parallel Performance Comparison

## Technologies
- Scala
- Parallel Collections

## Dataset
The dataset contains COVID-19 records in Russia with the following fields:
- Date                (DATE)
- Region/City         (VARCHAR)
- Region/City (Eng)   (VARCHAR)
- Region-ID           (INT)
- Day-Confirmed       (INT)
- Day-Deaths          (INT)
- Day-Recovered       (INT)
- Confirmed           (INT)
- Deaths              (INT)
- Recovered           (INT)

## Implemented Functions
1. **totalImpact**               –> Calculates total daily impact (confirmed + deaths + recovered).
2. **totalImpactParallel**       –> Parallel version of totalImpact using Scala parallel collections.
3. **dailySummary**              –> Aggregates total confirmed, deaths, and recovered per day.
4. **dailySummaryParallel**      –> Parallel version of dailySummary using Scala parallel collections.
5. **dailyCitySummary**          –> Aggregates COVID-19 statistics per city per day.
6. **dailyCitySummaryParallel**  –> Parallel version of dailyCitySummary using Scala parallel collections.

## Output Files
The program generates JSON output files:
- **result_functional.json**         (for totalImpact function)
- **result_parallel.json**           (for totalImpactParallel function)
- **daily_summary_functional.json**  (for dailySummary function)
- **daily_summary_parallel.json**    (for dailySummaryParallel function)
- **daily_city_functional.json**     (for dailyCitySummary function)
- **daily_city_parallel.json**       (for dailyCitySummaryParallel function)

## Performance Comparison
Sequential Processing vs Parallel Processing execution times are measured using System.nanoTime().

## How to Run the Project
This project is built using **Scala 3** and **sbt**.

To run the project:

```bash
sbt compile
sbt run
```

## Team Members
- 67070138 Puriwaj    –> Daily City Summary
- 67070146 Yasmina    –> Daily Summary Aggregation
- 67070150 Ratchanon  –> Total Impact
