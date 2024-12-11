```sql

    SELECT *
    FROM ids706_data_engineering.jcw131_nyc_shooting.jcw131_nyc_shooting_transformed
    WHERE Occur_Date = '04/19/2008'

```

```response from databricks
Query received, executing next...
```

```sql

    SELECT *
    FROM ids706_data_engineering.jcw131_nyc_shooting.jcw131_nyc_shooting_transformed
    WHERE Occur_Date = '04/19/2008'

```

```response from databricks
|   Incident_Key | Boro   | PERP_SEX   | Perp_Race                | VIC_SEX   | VIC_RACE       | OCCUR_DATE   |
|---------------:|:-------|:-----------|:-------------------------|:----------|:---------------|:-------------|
|       45789784 | QUEENS | M          | ASIAN / PACIFIC ISLANDER | M         | BLACK HISPANIC | 04/19/2008   |
|       45789781 | QUEENS |            |                          | M         | BLACK          | 04/19/2008   |
|       45789784 | QUEENS | M          | ASIAN / PACIFIC ISLANDER | M         | BLACK          | 04/19/2008   |
|       45789780 | QUEENS |            |                          | M         | BLACK          | 04/19/2008   |
|       45789784 | QUEENS | M          | ASIAN / PACIFIC ISLANDER | M         | BLACK          | 04/19/2008   |
|       45789780 | QUEENS |            |                          | M         | BLACK          | 04/19/2008   |
```

