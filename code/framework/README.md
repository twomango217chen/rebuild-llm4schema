### 项目概述
本框架用于基于历史负载的数据库模式优化，通过 LLM 多轮对话生成候选操作序列，并在每轮对话中执行合规性检查、存储估计、性能评估的闭环反馈，以降低总延迟。

### 技术栈与依赖
- 语言：Python 3.10+
- 数据库：MySQL 8.0+（所有 SQL 需兼容）
- 关键方法：EXPLAIN ANALYZE、基数缩放代价估计、回合内闭环评估
- 主要依赖：aiohttp、PyYAML、sqlparse、pymysql

### 快速开始
1. 配置环境变量：`LLM_API_KEY`、`LLM_API_URL`、`LLM_API_BASE`、`LLM_MODEL`。
2. 准备数据集目录，参考 [framework/docs/usage.md](framework/docs/usage.md)。
3. 修改配置文件 `framework/configs/default.yaml`。
4. 运行入口脚本：

```bash
python framework/scripts/run_pipeline.py --config framework/configs/default.yaml
```

### 仅改超参数/模型的批量实验
在不改搜索框架实现的前提下，可用批跑脚本做重复实验与模型切换：

```bash
python framework/scripts/run_experiment_batch.py \
  --config framework/configs/default.yaml \
  --experiment-name hp_model_grid_20260411 \
  --runs 3 \
  --alphas 0.05 0.1 \
  --max-nodes 80 120 \
  --models deepseek-chat deepseek-reasoner \
  --overrides-json '{"llm.temperature": 0.0, "llm.max_completion_tokens": 4096, "llm.timeout_sec": 180}'
```

输出目录：`output/experiments/<experiment-name>/`，总表在 `summary_all.json`。

### LLM接口可调参数（用于高质量模型）
`llm` 段新增可选字段（均不改搜索框架，仅改调用参数）：
- `model`: 直接指定模型名（如 `deepseek-reasoner`）。
- `temperature`, `top_p`, `max_completion_tokens`。
- `timeout_sec`, `request_retries`。
- `extra_headers`（JSON对象），`extra_body`（JSON对象）。

示例：

```yaml
llm:
  mode: live
  api_base: https://api.deepseek.com/v1
  model: deepseek-reasoner
  temperature: 0.0
  max_completion_tokens: 4096
  timeout_sec: 180
  request_retries: 2
```


### 目录结构
```
framework/
  configs/
  docs/
  examples/
  schemas/
  scripts/
  src/
```
