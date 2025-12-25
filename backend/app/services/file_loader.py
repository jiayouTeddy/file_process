from __future__ import annotations

import io
import os
from typing import List, Optional, Tuple

import pandas as pd


def detect_file_type(filename: str) -> str:
    # 根据文件扩展名判断类型（只允许 csv / excel）
    name = filename.lower()  # 转小写，避免大小写扩展名影响判断
    if name.endswith(".csv"):  # CSV
        return "csv"  # 返回 csv
    if name.endswith(".xlsx") or name.endswith(".xls"):  # Excel
        return "excel"  # 返回 excel
    raise ValueError("unsupported_file_type")  # 其它类型不支持


def get_excel_sheet_names(content: bytes) -> List[str]:
    # 获取 Excel 的 sheet 名列表（用于前端下拉选择）
    bio = io.BytesIO(content)  # 将 bytes 包装为文件对象
    try:
        xls = pd.ExcelFile(bio, engine="openpyxl")  # 优先用 openpyxl 读取 xlsx
        return list(xls.sheet_names)  # 返回 sheet 名列表
    except Exception:
        # 这里兜底：有些 .xls 需要 xlrd；若仍失败会继续抛异常
        bio.seek(0)  # 重置指针
        xls = pd.ExcelFile(bio, engine="xlrd")  # 使用 xlrd 读取 xls
        return list(xls.sheet_names)  # 返回 sheet 名列表


def _read_csv_with_fallback(content: bytes) -> pd.DataFrame:
    # 读取 CSV：尝试多种编码，提升中文环境兼容性
    # 注意：这里仅做常见兜底；真实生产可加入分隔符探测等更复杂逻辑
    encodings = ["utf-8-sig", "utf-8", "gbk"]  # 常见编码顺序
    last_err: Optional[Exception] = None  # 记录最后一次异常
    for enc in encodings:  # 逐个尝试编码
        try:
            bio = io.BytesIO(content)  # 重建 BytesIO（避免指针位置影响）
            df = pd.read_csv(  # 用 pandas 读取
                bio,  # 文件对象
                encoding=enc,  # 当前尝试编码
                keep_default_na=True,  # 保留 pandas 默认 NA 规则
                na_values=["", "NA", "N/A", "null", "None"],  # 常见空值标记
            )
            return df  # 成功则返回 DataFrame
        except Exception as e:  # 失败则记录异常继续
            last_err = e  # 保存异常
            continue  # 尝试下一个编码
    # 全部失败则抛出最后一个异常（便于定位问题）
    raise last_err or ValueError("csv_read_failed")


def read_file_to_df(content: bytes, file_type: str, sheet_name: Optional[str]) -> Tuple[pd.DataFrame, Optional[str]]:
    # 将文件内容读取为 DataFrame，并返回（df, selected_sheet）
    if file_type == "csv":  # CSV 分支
        df = _read_csv_with_fallback(content)  # 读取 CSV
        return df, None  # CSV 不存在 sheet
    if file_type == "excel":  # Excel 分支
        bio = io.BytesIO(content)  # 包装为 BytesIO
        # 如果没有指定 sheet，则默认取第一个（但你的产品选择是前端让用户选，所以这里仍支持兜底）
        if not sheet_name:  # 未指定 sheet
            xls = pd.ExcelFile(bio, engine="openpyxl")  # 打开 Excel
            sheet_name = xls.sheet_names[0] if xls.sheet_names else None  # 取第一个 sheet
            bio.seek(0)  # 重置指针
        # 使用 pandas 读取指定 sheet
        df = pd.read_excel(bio, sheet_name=sheet_name, engine=None)  # engine=None 让 pandas 自行选择
        return df, sheet_name  # 返回 df 与最终选择的 sheet
    raise ValueError("unsupported_file_type")  # 理论上不会走到这里


