from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class UploadFileInfo(BaseModel):
    # 文件唯一 ID（后端生成，用于后续 parse/normalize/setops 指定文件）
    file_id: str
    # 原始文件名（前端用于展示）
    filename: str
    # 文件类型：csv 或 excel（由后端根据扩展名判断）
    file_type: Literal["csv", "excel"]
    # 若为 Excel，返回可选 sheet 列表；CSV 则为 None
    sheet_names: Optional[List[str]] = None


class UploadResponse(BaseModel):
    # 本次会话 ID（后端生成，用于绑定同一批上传与后续操作）
    session_id: str
    # 上传成功的文件信息列表
    files: List[UploadFileInfo]


class ParseRequest(BaseModel):
    # 会话 ID
    session_id: str
    # 文件 ID
    file_id: str
    # Excel 的 sheet 名称（CSV 可不填/忽略）
    sheet_name: Optional[str] = None


class NaCell(BaseModel):
    # 行号（1-based，方便用户与表格对照）
    row: int
    # 列名
    col: str


class ParseResponse(BaseModel):
    # 原始列名列表（解析后 DataFrame 的列顺序）
    columns_original: List[str]
    # 建议规范列名（与 columns_original 一一对应；前端可编辑后提交）
    columns_suggestions: List[str]
    # NA 单元格位置列表（可能很多，后端会做上限截断）
    na_cells: List[NaCell]
    # 预览行（前 N 行，每行是 {列名: 值} 的 dict）
    preview_rows: List[Dict[str, object]]


class NormalizeRequest(BaseModel):
    # 会话 ID
    session_id: str
    # 文件 ID
    file_id: str
    # 列名映射：原列名 -> 新列名（由前端编辑确认后提交）
    rename_map: Dict[str, str] = Field(default_factory=dict)


class NormalizeResponse(BaseModel):
    # 应用映射后的列名列表（最终规范化结果）
    columns_normalized: List[str]


SetOp = Literal["intersection", "difference", "symmetric_difference"]


class SetOpsRequest(BaseModel):
    # 会话 ID
    session_id: str
    # 参与运算的文件 ID 列表（至少 2 个）
    file_ids: List[str] = Field(min_length=2)
    # 运算的列名（要求每个选中文件都存在该列）
    column_name: str
    # 运算类型：交集/差集/对称差集
    op: SetOp
    # 当 op 为 difference 时，必须指定 base 文件（做 base - others）
    base_file_id: Optional[str] = None
    # 是否丢弃 NA 值（默认 true：集合运算不包含 NA）
    drop_na: bool = True


class SetOpsResponse(BaseModel):
    # 结果 ID（用于导出下载）
    result_id: str
    # 结果数量
    count: int
    # 预览值（前 N 个）
    values_preview: List[object]


class ExportRawRequest(BaseModel):
    # 会话 ID
    session_id: str
    # 结果 ID（集合运算的结果，包含 ID 列表）
    result_id: str
    # 要导出的文件 ID 列表（选择哪些原始文件）
    file_ids: List[str] = Field(min_length=1)
    # 用于筛选的列名（默认为 "patient_id"，可配置）
    column_name: str = "patient_id"


