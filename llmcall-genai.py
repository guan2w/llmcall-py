#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LLM 批量调用：读取 Excel（prompt & QA），按 QA 的 Q 列逐行请求 LLM，
将 JSON 数组结果展开写回原文件。满足：
- 展开结果的所有行：Q 与 是否找到 相同
- 每个输入处理完成后立即落盘
- 支持 rows 范围、断点续跑、并发请求（请求并发，写入串行）

本版本使用 Google GenAI SDK (google-genai)
"""

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from google import genai
from google.genai import types
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# --- 列名配置 ---
COL_FOUND = "FOUND"  # 结果状态列名
COL_ERROR = "ERROR"      # 错误信息列名

# --- 配置解析（tomllib 优先） ---
try:
    import tomllib  # py311+
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # py310-
    except Exception:
        tomllib = None


def log(msg: str) -> None:
    now = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def mask_key_tail(key: Optional[str]) -> str:
    if not key:
        return "(empty)"
    tail = key[-5:] if len(key) >= 5 else key
    return "*" * max(0, len(key) - 5) + tail


def load_config(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"配置文件不存在: {path}")
    if tomllib is None:
        raise RuntimeError("缺少 tomllib/tomli，请安装 tomli 或使用 Python 3.11+")
    with open(path, "rb") as f:
        return tomllib.load(f)


def merge_llm_config(cfg: dict, llm_name: str, cli_api_key: Optional[str]) -> dict:
    base = cfg.get("llm", {}) or {}
    # TOML 中 [llm.gemini_search] 会被解析为嵌套表 cfg["llm"]["gemini_search"]
    # 支持两种写法：
    # 1) [llm] + [llm.gemini_search] - 标准嵌套表
    # 2) cfg.get("llm.gemini_search", {}) - 容错（某些解析器可能支持）
    llm_section = cfg.get("llm", {}) or {}
    if isinstance(llm_section, dict):
        by_table = llm_section.get(llm_name, {})  # 标准方式：cfg["llm"]["gemini_search"]
    else:
        by_table = {}
    # 容错：尝试直接键访问（某些 TOML 解析器可能支持）
    if not by_table:
        by_table = cfg.get(f"llm.{llm_name}", {}) or {}

    # 合并：base <- by_table
    merged = dict(base)
    if isinstance(by_table, dict):
        merged.update(by_table)

    # CLI api_key 优先
    if cli_api_key:
        merged["api_key"] = cli_api_key

    # 必要字段检查
    # api_base 为可选，如果提供则用于自定义 API 端点
    api_base = merged.get("api_base")
    api_key = merged.get("api_key")
    model_id = merged.get("model_id")
    if not (api_key or merged.get("user_token")):
        raise ValueError("未提供 api_key（或 user_token）")
    if not model_id:
        raise ValueError("配置缺少 llm.model_id")

    # 默认并发/重试/超时
    merged.setdefault("parallel", 5)
    merged.setdefault("retry_times", 1)
    merged.setdefault("retry_delay", 10)
    merged.setdefault("timeout", 120)
    return merged


def parse_rows_arg(rows_arg: Optional[str], data_start_row: int, data_end_row: int) -> List[int]:
    """
    rows 语法：
      - None: 处理 data_start_row..data_end_row
      - "2-5": 处理 2..5
      - "2+":  处理 2..data_end_row
    返回：原始行号列表（基于启动时的行号）
    """
    if not rows_arg:
        return list(range(data_start_row, data_end_row + 1))

    rows_arg = rows_arg.strip()
    m = re.fullmatch(r"(\d+)\-(\d+)", rows_arg)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        a = max(a, data_start_row)
        b = min(b, data_end_row)
        if a > b:
            return []
        return list(range(a, b + 1))

    m = re.fullmatch(r"(\d+)\+", rows_arg)
    if m:
        a = int(m.group(1))
        a = max(a, data_start_row)
        return list(range(a, data_end_row + 1))

    raise ValueError(f"rows 参数不合法: {rows_arg}")


def get_sheet(wb, name: str) -> Worksheet:
    if name not in wb.sheetnames:
        raise ValueError(f"Excel 缺少工作表: {name}")
    return wb[name]


def find_header_indexes(ws: Worksheet) -> Dict[str, int]:
    """
    扫描第1行，返回：列名 -> 列索引（1-based）
    """
    headers = {}
    for col in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=col).value
        if v is None:
            continue
        headers[str(v).strip()] = col
    return headers


def ensure_columns(ws: Worksheet, headers: Dict[str, int], need_cols: List[str]) -> Dict[str, int]:
    """
    确保 need_cols 存在于表头，不存在则在末尾追加。返回更新后的列映射。
    """
    updated = dict(headers)
    for name in need_cols:
        if name not in updated:
            ws.cell(row=1, column=ws.max_column + 1, value=name)
            updated[name] = ws.max_column  # 刚写入的单元格已经生效
    return updated


def compact_preview(text: str, limit: int = 30) -> str:
    text = (text or "").replace("\n", " ").strip()
    return text if len(text) <= limit else text[:limit] + "..."


def is_json_array_text(s: str) -> bool:
    s = s.strip()
    return s.startswith("[") and s.endswith("]")


def extract_json_array_from_text(s: str) -> str:
    """
    兼容模型把 JSON 放在 ```json ... ``` 或前后有说明文字的情况。
    策略：
      1) 去除 ```...``` 包裹
      2) 从文本中找到最外层方括号的 JSON 段
    """
    text = s.strip()

    # 去除 ```json ... ``` 包裹
    fence = re.compile(r"^```(?:json|JSON)?\s*(.*?)\s*```$", re.S)
    m = fence.match(text)
    if m:
        text = m.group(1).strip()

    if is_json_array_text(text):
        return text

    # 宽松：从首个 '[' 到最后一个 ']' 的包裹
    lb = text.find("[")
    rb = text.rfind("]")
    if lb != -1 and rb != -1 and rb > lb:
        candidate = text[lb:rb + 1].strip()
        if is_json_array_text(candidate):
            return candidate

    # 失败则返回原文（让上层报错）
    return text


def call_llm_genai(
    client: genai.Client,
    model: str,
    system_prompt: str,
    user_content: str,
    timeout: int,
) -> Tuple[Optional[List[Dict[str, Any]]], Dict[str, Any], Optional[str]]:
    """
    调用 Google GenAI SDK 的 generate_content 接口。
    返回：(json_array或None, usage字典, 错误文本或None)
    
    注意：timeout 参数保留在函数签名中以保持接口一致性，
    但 Google GenAI SDK 的 generate_content 可能不直接支持该参数。
    超时控制可能需要通过 Client 配置或其他方式实现。
    """
    try:
        # 构建调用参数
        # Google GenAI SDK 的 generate_content 支持 system_instruction 参数
        # 如果不支持，则将 system prompt 前置到 user content
        call_kwargs = {
            "model": model,
            "contents": user_content,
        }
        
        # 尝试添加 system_instruction 参数（如果 SDK 支持）
        if system_prompt:
            try:
                # 先尝试使用 system_instruction 参数
                call_kwargs["system_instruction"] = system_prompt
                response = client.models.generate_content(**call_kwargs)
            except (TypeError, AttributeError):
                # 如果 system_instruction 不支持，将 system prompt 合并到 contents
                call_kwargs.pop("system_instruction", None)
                call_kwargs["contents"] = f"{system_prompt}\n\n{user_content}"
                response = client.models.generate_content(**call_kwargs)
        else:
            response = client.models.generate_content(**call_kwargs)
        
    except Exception as e:
        return None, {}, f"请求异常: {type(e).__name__}: {e}"

    # 提取响应文本
    try:
        content = response.text
    except Exception as e:
        return None, {}, f"响应缺少 text 属性: {type(e).__name__}: {e}"

    # 提取 usage 信息（如果存在）
    usage = {}
    try:
        # 尝试多种可能的 usage 属性路径
        if hasattr(response, 'usage_metadata') and response.usage_metadata:
            usage_meta = response.usage_metadata
            usage = {
                "prompt_tokens": getattr(usage_meta, 'prompt_token_count', 0) or 0,
                "completion_tokens": getattr(usage_meta, 'completion_token_count', 0) or 0,
                "total_tokens": getattr(usage_meta, 'total_token_count', 0) or 0,
            }
        elif hasattr(response, 'usage') and response.usage:
            # 兼容其他可能的 usage 格式
            usage_obj = response.usage
            usage = {
                "prompt_tokens": getattr(usage_obj, 'prompt_tokens', 0) or getattr(usage_obj, 'input_tokens', 0) or 0,
                "completion_tokens": getattr(usage_obj, 'completion_tokens', 0) or getattr(usage_obj, 'output_tokens', 0) or 0,
                "total_tokens": getattr(usage_obj, 'total_tokens', 0) or 0,
            }
    except Exception:
        # 如果无法提取 usage，继续执行（usage 为空字典）
        pass

    # 解析 JSON 数组
    content = extract_json_array_from_text(str(content))
    try:
        arr = json.loads(content)
    except Exception as e:
        return None, usage, f"内容不是 JSON 数组: {type(e).__name__}: {e}; 原文片段: {content[:1000]}"

    if not isinstance(arr, list):
        return None, usage, "顶层非数组"
    # 元素必须为对象
    for i, it in enumerate(arr):
        if not isinstance(it, dict):
            return None, usage, f"数组第 {i+1} 个元素不是对象"
    return arr, usage, None


def with_retry(func, retry_times: int, retry_delay: int):
    def wrapper(*args, **kwargs):
        last_err = None
        for i in range(retry_times + 1):
            result = func(*args, **kwargs)
            # 约定：func 返回 (arr, usage, err_text)
            if result[2] is None:
                return result
            last_err = result[2]
            # 对可重试错误做简单判断（含 429/5xx 文本时退避），否则也简单等一等
            time.sleep(retry_delay if i < retry_times else 0)
        return (None, {}, last_err)
    return wrapper


def save_with_backup_atomic(wb, xlsx_path: str, made_backup: List[bool]) -> None:
    """
    首次保存前做 .bak 备份；使用临时文件 + 替换 的基本原子写法
    """
    if not made_backup[0]:
        bak = xlsx_path + ".bak"
        if not os.path.exists(bak):
            try:
                with open(xlsx_path, "rb") as rf, open(bak, "wb") as wf:
                    wf.write(rf.read())
                log(f"已创建备份: {bak}")
            except Exception as e:
                log(f"创建备份失败（忽略）: {e}")
        made_backup[0] = True

    tmp = xlsx_path + ".tmp"
    wb.save(tmp)
    # Windows 下替换
    try:
        if os.path.exists(xlsx_path):
            os.replace(tmp, xlsx_path)
        else:
            os.rename(tmp, xlsx_path)
    except Exception as e:
        log(f"保存替换失败: {e}")
        # 兜底直接写原文件（可能失败）
        wb.save(xlsx_path)


def main():
    parser = argparse.ArgumentParser(description="批量调用 LLM 并写回 Excel（Google GenAI SDK 版本）")
    parser.add_argument("--input-file", required=True, help="输入 Excel 文件路径")
    parser.add_argument("--config", default="config.toml", help="配置文件路径，默认 config.toml")
    parser.add_argument("--llm", required=True, help="使用的模型配置名，例如 genai_2_5_flash_latest")
    parser.add_argument("--rows", default=None, help="处理行范围，例如 2-5 或 2+；缺省处理全部")
    parser.add_argument("--api-key", default=None, help="可选；命令行覆盖配置中的 api_key")
    args = parser.parse_args()

    xlsx_path = args.input_file
    if not os.path.exists(xlsx_path):
        print(f"找不到输入文件: {xlsx_path}", file=sys.stderr)
        sys.exit(2)

    # 读配置
    cfg = load_config(args.config)
    llm_cfg = merge_llm_config(cfg, args.llm, args.api_key)

    api_key = llm_cfg.get("api_key") or ""
    model_id = llm_cfg["model_id"]
    parallel = int(llm_cfg.get("parallel", 5))
    retry_times = int(llm_cfg.get("retry_times", 1))
    retry_delay = int(llm_cfg.get("retry_delay", 10))
    timeout = int(llm_cfg.get("timeout", 120))
    price_in = float(llm_cfg.get("price_per_1m_input_tokens", 0.0))
    price_out = float(llm_cfg.get("price_per_1m_output_tokens", 0.0))

    api_base = llm_cfg.get("api_base")
    
    log("启动参数：")
    log(f"- input-file: {xlsx_path}")
    log(f"- llm: {args.llm}")
    log(f"- model_id: {model_id}")
    log(f"- api_key: {mask_key_tail(api_key)}")
    if api_base:
        log(f"- api_base: {api_base}")
    log(f"- parallel: {parallel}, retry_times: {retry_times}, retry_delay: {retry_delay}s, timeout: {timeout}s")
    if args.rows:
        log(f"- rows: {args.rows}")

    # 创建 Google GenAI 客户端
    try:
        # 如果提供了 api_base，使用 http_options 自定义端点
        if api_base:
            client = genai.Client(
                api_key=api_key,
                http_options=types.HttpOptions(base_url=api_base)
            )
        else:
            client = genai.Client(api_key=api_key)
    except Exception as e:
        print(f"无法创建 GenAI 客户端：{e}", file=sys.stderr)
        sys.exit(2)

    # 读 Excel
    try:
        wb = load_workbook(xlsx_path)
    except Exception as e:
        print(f"无法打开 Excel：{e}", file=sys.stderr)
        sys.exit(2)

    ws_prompt = get_sheet(wb, "prompt")
    ws_qa = get_sheet(wb, "QA")

    # 系统提示（prompt!A1）
    sys_prompt = ws_prompt["A1"].value
    if sys_prompt is None or str(sys_prompt).strip() == "":
        print("prompt!A1 不能为空", file=sys.stderr)
        sys.exit(2)
    sys_prompt = str(sys_prompt)

    # QA 表头
    headers = find_header_indexes(ws_qa)
    if "Q" not in headers:
        print("QA 页缺少表头列：Q", file=sys.stderr)
        sys.exit(2)
    # 确保必要列
    headers = ensure_columns(ws_qa, headers, [COL_FOUND, COL_ERROR])
    col_Q = headers["Q"]
    col_found = headers[COL_FOUND]
    col_err = headers[COL_ERROR]

    # 输出字段集合：表头中除去 Q/是否找到/错误 的其它列（仅写这些）
    output_cols = {k: v for k, v in headers.items() if k not in ("Q", COL_FOUND, COL_ERROR)}

    data_start_row = 2
    data_end_row_initial = ws_qa.max_row  # 启动时的原始末行（用于 rows 范围）
    target_rows = parse_rows_arg(args.rows, data_start_row, data_end_row_initial)

    # 统计去重 Q：全部候选 + 已完成
    q_all = []
    q_done_set = set()
    for r in target_rows:
        qv = ws_qa.cell(row=r, column=col_Q).value
        if qv is None or str(qv).strip() == "":
            continue
        q_all.append(str(qv))
        found_v = ws_qa.cell(row=r, column=col_found).value
        if found_v is not None and str(found_v).strip() != "":
            q_done_set.add(str(qv))
    q_all_unique = set(q_all)
    log(f"候选 Q 去重统计：总 {len(q_all_unique)}，其中已完成 {len(q_done_set)}")

    # 为 rows 范围执行插入偏移跟踪：记录"原始主行" -> 插入的额外行数
    inserted_below: Dict[int, int] = {}

    made_backup = [False]

    # 简单的进度累计
    total = len(target_rows)
    n_done = 0
    n_success = 0  # 有结果
    n_empty = 0    # 数组空
    n_error = 0

    # 费用累计（当 usage 存在时）
    sum_prompt_tokens = 0
    sum_completion_tokens = 0

    retry_call = with_retry(
        lambda *a, **kw: call_llm_genai(*a, **kw),
        retry_times=retry_times,
        retry_delay=retry_delay,
    )

    def current_row_pos(original_row: int) -> int:
        """根据已插入情况，计算该原始行现在的实际行号"""
        shift = 0
        for r0, added in inserted_below.items():
            if r0 < original_row:
                shift += added
        return original_row + shift

    for idx, r0 in enumerate(target_rows, start=1):
        r = current_row_pos(r0)
        qv = ws_qa.cell(row=r, column=col_Q).value
        qtext = "" if qv is None else str(qv).strip()

        # 判定是否跳过（断点续跑：主行 是否找到 非空就跳过）
        found_val = ws_qa.cell(row=r, column=col_found).value
        if found_val is not None and str(found_val).strip() != "":
            n_done += 1
            log(f"{idx}/{total} 跳过（已完成） r={r} Q='{compact_preview(qtext)}'")
            continue

        if qtext == "":
            # 空 Q：标记错误并继续
            ws_qa.cell(row=r, column=col_found, value="错误")
            ws_qa.cell(row=r, column=col_err, value="Q 为空")
            save_with_backup_atomic(wb, xlsx_path, made_backup)
            n_done += 1
            n_error += 1
            log(f"{idx}/{total} 错误：Q 为空（r={r}），已落盘")
            continue

        # 请求
        arr, usage, err = retry_call(
            client, model_id, sys_prompt, qtext, timeout
        )

        if usage:
            sum_prompt_tokens += int(usage.get("prompt_tokens", 0))
            sum_completion_tokens += int(usage.get("completion_tokens", 0))

        if err is not None:
            # 写入主行错误
            ws_qa.cell(row=r, column=col_found, value="错误")
            ws_qa.cell(row=r, column=col_err, value=str(err)[:500])
            save_with_backup_atomic(wb, xlsx_path, made_backup)
            n_done += 1
            n_error += 1
            log(f"{idx}/{total} 错误 r={r} Q='{compact_preview(qtext)}' -> {err}")
            continue

        # arr 一定是 list[dict]
        if len(arr) == 0:
            # 无结果：主行写"否"，不插入新行
            ws_qa.cell(row=r, column=col_found, value="否")
            ws_qa.cell(row=r, column=col_err, value="")
            # 清空输出列
            for name, c in output_cols.items():
                ws_qa.cell(row=r, column=c, value="")
            save_with_backup_atomic(wb, xlsx_path, made_backup)
            inserted_below[r0] = 0
            n_done += 1
            n_empty += 1
            log(f"{idx}/{total} 空结果 r={r} Q='{compact_preview(qtext)}'（已落盘）")
            continue

        # 有结果：主行写第1个，下面插入 len(arr)-1 行写其余
        # 所有展开行的 Q 与 是否找到 相同
        ws_qa.cell(row=r, column=col_Q, value=qtext)
        ws_qa.cell(row=r, column=col_found, value="是")
        ws_qa.cell(row=r, column=col_err, value="")
        # 写输出字段
        first_obj = arr[0]
        for name, c in output_cols.items():
            v = first_obj.get(name, "")
            if isinstance(v, (dict, list)):
                v = json.dumps(v, ensure_ascii=False)
            ws_qa.cell(row=r, column=c, value=v)

        extra = max(0, len(arr) - 1)
        if extra > 0:
            ws_qa.insert_rows(r + 1, amount=extra)
            # 逐条写入
            for i in range(extra):
                rr = r + 1 + i
                ws_qa.cell(row=rr, column=col_Q, value=qtext)
                ws_qa.cell(row=rr, column=col_found, value="是")
                ws_qa.cell(row=rr, column=col_err, value="")
                obj = arr[1 + i]
                for name, c in output_cols.items():
                    v = obj.get(name, "")
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, ensure_ascii=False)
                    ws_qa.cell(row=rr, column=c, value=v)

        inserted_below[r0] = extra
        save_with_backup_atomic(wb, xlsx_path, made_backup)
        n_done += 1
        n_success += 1
        log(f"{idx}/{total} 成功 r={r} Q='{compact_preview(qtext)}' 展开 {len(arr)} 行（已落盘）")

    # 结束统计
    cost = 0.0
    if price_in or price_out:
        cost = (sum_prompt_tokens / 1_000_000.0) * price_in + (sum_completion_tokens / 1_000_000.0) * price_out

    log("完成。")
    log(f"- 总计：{total}, 成功(有结果)={n_success}, 空结果={n_empty}, 错误={n_error}")
    if (sum_prompt_tokens + sum_completion_tokens) > 0:
        log(f"- tokens: prompt={sum_prompt_tokens}, completion={sum_completion_tokens}, 估算费用=${cost:.4f}（按配置单价）")


if __name__ == "__main__":
    main()

