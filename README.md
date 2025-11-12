

## 操作说明
```sh
# 完整执行
python llmcall.py --input-file ./pq01.xlsx --llm gemini_search

# 验证 3 行
python llmcall.py --input-file ./data/listed-1111.xlsx --llm gemini_search --rows 2-4
python llmcall.py --input-file ./data/listed-president-1111.xlsx --llm gemini_search --rows 2-4
```
