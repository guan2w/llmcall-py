

## 操作说明
```sh
# 完整执行
python llmcall.py --input-file ./pq01.xlsx --llm gemini_search

# 验证 3 行
python llmcall.py --input-file ./data/listed-1111.xlsx --llm gemini_search --rows 2-4
python llmcall.py --input-file ./data/listed-president-1111.xlsx --llm gemini_search --rows 2-4

# V-API: gemini
python llmcall.py --input-file data/unlisted-en-prompt-1113/unlisted-1113.xlsx --llm gemini_2_5_flash_search --rows 2-4
python llmcall.py --input-file data/unlisted-en-prompt-1113/unlisted-1113.xlsx --llm gemini_2_5_pro_search --rows 2-4
python llmcall.py --input-file data/unlisted-en-prompt-1113/unlisted-1113.xlsx --llm gemini_pro_latest_search --rows 2-4

# google genai
python llmcall-genai.py --input-file data/unlisted-en-prompt-1113/unlisted-1113.xlsx --llm genai_2_5_flash --rows 2-4

```
