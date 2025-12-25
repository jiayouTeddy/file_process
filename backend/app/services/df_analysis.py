from __future__ import annotations

import re
from typing import Dict, List

import pandas as pd


def suggest_column_names(columns: List[str]) -> List[str]:
    """
    为每个原始列名生成“建议规范名”。

    注意：
    - 这里仅生成建议，不会自动应用（你在前端手动确认/修改后才应用）。
    - 规则尽量简单可解释：去空白、空白转下划线、去特殊字符、统一小写。
    """

    suggestions: List[str] = []  # 最终建议列表
    used: Dict[str, int] = {}  # 用于处理重复建议名：name -> 已用次数

    for c in columns:  # 遍历每一个原始列名
        raw = str(c)  # 强制转字符串，避免列名是数字/时间类型
        s = raw.strip()  # 去掉首尾空格
        s = re.sub(r"\s+", "_", s)  # 连续空白替换为下划线
        s = re.sub(r"[^\w_]", "", s)  # 去掉非字母数字下划线字符
        s = s.lower()  # 转小写
        if s == "":  # 若清洗后为空
            s = "col"  # 给一个兜底名称

        # 处理“建议名重复”的情况：加后缀 _2/_3...
        if s in used:  # 若已经被使用过
            used[s] += 1  # 次数+1
            s2 = f"{s}_{used[s]}"  # 构造带后缀的新建议名
            suggestions.append(s2)  # 添加
        else:  # 未使用过
            used[s] = 1  # 标记使用
            suggestions.append(s)  # 添加

    return suggestions  # 返回建议列表


def find_na_cells(df: pd.DataFrame, max_cells: int = 5000) -> List[Dict[str, object]]:
    """
    扫描 DataFrame 中的 NA/空值位置，返回 [{row, col}] 列表。

    约定：
    - row 使用 1-based（更贴合用户在 Excel/表格中看到的行号）
    - 输出可能非常多，因此做 max_cells 截断
    """

    # 为了让 row 与展示一致，这里用 reset_index(drop=True) 的逻辑来定义行号
    df2 = df.reset_index(drop=True)  # 重置索引，确保行号从 0 开始连续
    mask = df2.isna()  # 生成 NA 布尔矩阵
    stacked = mask.stack()  # 将二维矩阵压平成 (row_idx, col_name) -> bool

    out: List[Dict[str, object]] = []  # 输出列表
    count = 0  # 计数器
    for (row_idx, col_name), is_na in stacked.items():  # 遍历每个单元格的 NA 标记
        if not is_na:  # 如果不是 NA
            continue  # 跳过
        out.append(  # 记录 NA 位置
            {
                "row": int(row_idx) + 1,  # 转为 1-based
                "col": str(col_name),  # 列名转字符串
            }
        )
        count += 1  # 计数+1
        if count >= max_cells:  # 达到上限
            break  # 终止扫描

    return out  # 返回 NA 位置列表


def preview_rows(df: pd.DataFrame, n: int = 20) -> List[Dict[str, object]]:
    """
    生成 DataFrame 预览（前 n 行），返回 list[dict]，便于前端直接渲染表格。
    """

    df2 = df.reset_index(drop=True)  # 重置索引便于展示
    head = df2.head(n)  # 截取前 n 行
    rows = head.to_dict(orient="records")  # 转为 records（每行一个 dict）
    # pandas 可能输出 numpy 类型，这里不强制转换，让 FastAPI 的 JSON 编码器处理
    return rows  # 返回预览行


def apply_rename_map(df: pd.DataFrame, rename_map: Dict[str, str]) -> pd.DataFrame:
    """
    应用列名映射（原列名 -> 新列名），并做基本校验。

    规则：
    - 新列名不能为空（空字符串会被拒绝）
    - 新列名不能重复（避免覆盖）
    """

    # 先复制一份，避免原 df 被意外修改（更安全）
    df2 = df.copy()  # 复制 DataFrame

    # 构造最终映射：只对 df 中确实存在的列生效
    final_map: Dict[str, str] = {}  # 最终映射
    for old, new in rename_map.items():  # 遍历映射
        if old not in df2.columns:  # 原列名不存在
            continue  # 忽略
        new2 = str(new).strip()  # 新列名转字符串并去空白
        if new2 == "":  # 新列名为空
            raise ValueError(f"empty_column_name: {old}")  # 抛出错误
        final_map[str(old)] = new2  # 写入最终映射

    # 应用映射（pandas rename）
    df2 = df2.rename(columns=final_map)  # 重命名列

    # 检查重名列（pandas 允许重名，但我们这里禁止）
    cols = [str(c) for c in list(df2.columns)]  # 列名转字符串列表
    if len(cols) != len(set(cols)):  # 若存在重复
        raise ValueError("duplicated_columns_after_rename")  # 抛出错误

    return df2  # 返回重命名后的 DataFrame


