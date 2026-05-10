## 中文说明

以下为推荐数据集目录结构示例：

```
<dataset_root>/
  schema.sql
  insert.sql
  origin_csv/
  workload/
    sql/
      q0001.sql
      q0002.sql
    metrics.csv  # sql_id,freq,avg_latency_ms,total_latency_ms
    explain/
      q0001.txt
```

---

## English Notes

Recommended dataset layout example:

```
<dataset_root>/
  schema.sql
  insert.sql
  origin_csv/
  workload/
    sql/
      q0001.sql
      q0002.sql
    metrics.csv  # sql_id,freq,avg_latency_ms,total_latency_ms
    explain/
      q0001.txt
```
