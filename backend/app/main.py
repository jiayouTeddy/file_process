from __future__ import annotations

import io
import csv
import math
import numbers
import os
import zipfile
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from backend.app.models import (
    ExportRawRequest,
    NormalizeRequest,
    NormalizeResponse,
    ParseRequest,
    ParseResponse,
    SetOpsRequest,
    SetOpsResponse,
    UploadResponse,
)
from backend.app.services.df_analysis import apply_rename_map, find_na_cells, preview_rows, suggest_column_names
from backend.app.services.file_loader import detect_file_type, get_excel_sheet_names, read_file_to_df
from backend.app.services.set_ops import compute_set_op
from backend.app.services.store import store


app = FastAPI(title="File Column SetOps Web")  # 创建 FastAPI 应用

# 允许本地开发时前端直接访问（本项目静态文件同源也能用；这里作为兜底）
app.add_middleware(  # 添加 CORS 中间件
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源（演示项目；生产请收紧）
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有头
)


def _raise(status: int, code: str, detail: str) -> None:
    # 统一抛 HTTPException 的小工具函数（便于保持返回风格一致）
    raise HTTPException(status_code=status, detail={"code": code, "detail": detail})


def _export_value_to_text(v: object) -> str:
    """
    将单个值转换为“用于导出的文本”。

    目标（对应你的反馈）：
    - Excel 导出不应该出现 39882.0 这种“整数浮点”显示
    - 因此：如果值是“浮点但整数值”（例如 39882.0），导出为 "39882"
    - 其它类型：尽量保持可读的字符串形式

    说明：
    - 这里同时兼容 numpy 标量（pandas 常见），用 numbers.Real/Integral 做判断
    - None/NA -> 空字符串
    """

    # 1) 统一处理 NA/None
    if v is None or pd.isna(v):  # None 或 pandas 认为的 NA
        return ""  # 导出为空字符串

    # 2) 字符串直接返回（避免破坏前导零等）
    if isinstance(v, str):  # 原本就是字符串
        return v  # 直接返回

    # 3) bool 单独处理（bool 是 Integral 的子类，避免被当成 0/1）
    if isinstance(v, bool):  # 布尔值
        return "True" if v else "False"  # 转字符串

    # 4) 整数（含 numpy 整数）直接转 int 再转字符串
    if isinstance(v, numbers.Integral):  # 整数类型
        return str(int(v))  # 转为整数字符串

    # 5) 实数（含 numpy 浮点等）：若是整数值的浮点，去掉 .0
    if isinstance(v, numbers.Real):  # 实数类型（非整数）
        fv = float(v)  # 转为 Python float
        if math.isfinite(fv) and fv.is_integer():  # 有限且是整数值
            return str(int(fv))  # 去掉 .0
        return str(fv)  # 保留小数

    # 6) 其它类型兜底：转字符串
    return str(v)


@app.post("/api/upload", response_model=UploadResponse)
async def api_upload(
    session_id: Optional[str] = Form(None),
    files: List[UploadFile] = File(...),
) -> UploadResponse:
    # 上传接口：接收多文件并缓存；若 Excel 额外返回 sheet_names
    # 说明：
    # - 第一次上传：session_id 为空，后端创建新会话
    # - 追加上传：前端带 session_id，后端把文件追加到同一个会话中（解决“分多次上传导致只剩一个文件”的问题）
    store.cleanup()  # 每次请求顺便做一次过期清理（简单实现）
    if session_id:  # 若前端传了 session_id（追加上传）
        try:
            store.get_session(session_id)  # 校验会话存在，并 touch
        except KeyError:
            # 会话过期/不存在：降级为创建新会话
            session_id = store.create_session()
    else:
        # 未传 session_id：创建新会话
        session_id = store.create_session()  # 创建新会话

    out_files = []  # 输出文件信息列表
    for f in files:  # 遍历上传文件
        if not f.filename:  # 若文件名为空
            _raise(400, "bad_filename", "文件名为空")  # 抛错
        try:
            file_type = detect_file_type(f.filename)  # 判断文件类型
        except ValueError:
            _raise(400, "unsupported_file_type", "仅支持上传 excel/csv 文件")  # 类型不支持

        content = await f.read()  # 读取文件 bytes
        try:
            file_id = store.add_file(session_id, f.filename, file_type, content)  # 存入缓存
        except ValueError as e:
            _raise(400, str(e), f"上传失败：{e}")  # 返回限制错误

        sheet_names: Optional[List[str]] = None  # 默认没有 sheet_names
        if file_type == "excel":  # Excel 才需要取 sheet
            try:
                sheet_names = get_excel_sheet_names(content)  # 解析 sheet 列表
            except Exception as e:
                _raise(400, "excel_parse_failed", f"无法读取 Excel 的 sheet 信息：{e}")  # 解析失败
            store.set_excel_sheet_names(session_id, file_id, sheet_names)  # 写入缓存

        out_files.append(  # 汇总返回信息
            {
                "file_id": file_id,
                "filename": f.filename,
                "file_type": file_type,
                "sheet_names": sheet_names,
            }
        )

    return UploadResponse(session_id=session_id, files=out_files)  # 返回响应


@app.post("/api/parse", response_model=ParseResponse)
async def api_parse(req: ParseRequest) -> ParseResponse:
    # 解析接口：按 file_id + (sheet_name) 读取为 DataFrame，并返回分析信息
    store.cleanup()  # 清理过期会话
    try:
        sf = store.get_file(req.session_id, req.file_id)  # 获取文件缓存
    except KeyError:
        _raise(404, "file_not_found", "找不到该文件，请重新上传")  # 文件不存在

    # 若为 Excel，必须由前端选择 sheet（这里也做后端校验）
    if sf.file_type == "excel":  # Excel
        if not req.sheet_name:  # 没传 sheet
            _raise(400, "sheet_required", "Excel 文件必须选择一个 sheet")  # 抛错
        if sf.sheet_names and req.sheet_name not in sf.sheet_names:  # sheet 不存在
            _raise(400, "sheet_not_found", "选择的 sheet 不存在")  # 抛错

    try:
        df, selected_sheet = read_file_to_df(sf.content, sf.file_type, req.sheet_name)  # 读入 DataFrame
    except Exception as e:
        _raise(400, "read_failed", f"读取文件失败：{e}")  # 读取失败

    # 为了让行号逻辑稳定，这里统一把 index 重置
    df = df.reset_index(drop=True)  # 重置索引

    # 缓存 df
    store.put_df(req.session_id, req.file_id, df, selected_sheet)  # 存入缓存

    # 生成列名与建议
    cols = [str(c) for c in list(df.columns)]  # 原始列名
    suggestions = suggest_column_names(cols)  # 建议规范名

    # NA 扫描
    na_cells = find_na_cells(df, max_cells=store.max_na_cells)  # 扫描 NA

    # 预览
    preview = preview_rows(df, n=store.max_preview_rows)  # 生成预览

    return ParseResponse(  # 返回解析结果
        columns_original=cols,
        columns_suggestions=suggestions,
        na_cells=na_cells,  # pydantic 会自动把 dict 转为 NaCell
        preview_rows=preview,
    )


@app.post("/api/normalize", response_model=NormalizeResponse)
async def api_normalize(req: NormalizeRequest) -> NormalizeResponse:
    # 列名规范化接口：应用前端提交的 rename_map 并返回最终列名
    store.cleanup()  # 清理过期会话
    try:
        df = store.get_df(req.session_id, req.file_id)  # 获取 DataFrame
    except KeyError:
        _raise(404, "df_not_found", "尚未解析该文件，请先解析")  # 还没 parse

    try:
        df2 = apply_rename_map(df, req.rename_map)  # 应用列名映射
    except ValueError as e:
        _raise(400, "rename_failed", str(e))  # 重命名失败

    # 覆盖缓存
    try:
        sf = store.get_file(req.session_id, req.file_id)  # 获取文件信息
        store.put_df(req.session_id, req.file_id, df2, sf.selected_sheet)  # 更新缓存
    except KeyError:
        store.put_df(req.session_id, req.file_id, df2, None)  # 兜底写入

    cols2 = [str(c) for c in list(df2.columns)]  # 获取规范化后的列名
    return NormalizeResponse(columns_normalized=cols2)  # 返回结果


@app.post("/api/setops", response_model=SetOpsResponse)
async def api_setops(req: SetOpsRequest) -> SetOpsResponse:
    # 集合运算接口：对多个文件同名列的“唯一值集合”做交集/差集/对称差集
    store.cleanup()  # 清理过期会话

    # 获取所有 df
    dfs = []  # DataFrame 列表
    for fid in req.file_ids:  # 遍历 file_ids
        try:
            df = store.get_df(req.session_id, fid)  # 获取 df
        except KeyError:
            _raise(404, "df_not_found", "存在未解析的文件，请先解析并规范化")  # 缺 df
        dfs.append(df)  # 记录

    # 差集需要 base_file_id
    base_index: Optional[int] = None  # base 索引
    if req.op == "difference":  # 差集
        if not req.base_file_id:  # 未提供 base
            _raise(400, "base_required", "difference 运算必须指定 base_file_id")  # 抛错
        if req.base_file_id not in req.file_ids:  # base 不在选中列表
            _raise(400, "base_invalid", "base_file_id 必须在 file_ids 中")  # 抛错
        base_index = req.file_ids.index(req.base_file_id)  # 找到 base 的位置

    # 计算集合运算
    try:
        values = compute_set_op(  # 执行运算
            dfs=dfs,
            column_name=req.column_name,
            op=req.op,
            drop_na=req.drop_na,
            base_index=base_index,
        )
    except KeyError as e:
        _raise(400, "column_not_found", str(e))  # 列不存在
    except ValueError as e:
        _raise(400, "setops_failed", str(e))  # 运算失败

    # 缓存结果
    try:
        result_id = store.put_result(req.session_id, values)  # 写入结果缓存
    except ValueError as e:
        _raise(400, "result_too_large", str(e))  # 结果太大

    # 生成预览（最多前 100 个）
    preview = values[:100]  # 截断预览
    return SetOpsResponse(result_id=result_id, count=len(values), values_preview=preview)  # 返回响应


@app.get("/api/export")
async def api_export(session_id: str, result_id: str, format: str = "csv") -> StreamingResponse:
    # 导出接口：下载结果为 csv 或 xlsx
    store.cleanup()  # 清理过期
    try:
        res = store.get_result(session_id, result_id)  # 获取结果
    except KeyError:
        _raise(404, "result_not_found", "找不到结果，请重新计算")  # 结果不存在

    # 组装为单列表格，便于导出
    # 需求：导出文件默认为“字符型/文本”格式
    # 说明：
    # - CSV：无法强制 Excel 一定不做自动类型识别，但我们会把内容按字符串写出并全量加引号
    # - XLSX：使用 xlsxwriter 把整列格式设置为“文本(@)”，Excel 打开也会按文本显示
    values_as_text = [_export_value_to_text(v) for v in res.values]  # 统一格式化为文本（解决 .0 问题）
    df = pd.DataFrame({"value": values_as_text})  # 单列 DataFrame（全为字符串）

    fmt = (format or "csv").lower()  # 统一小写
    if fmt == "csv":  # 导出 CSV
        buf = io.StringIO()  # 文本缓冲
        df.to_csv(  # 写入 CSV
            buf,
            index=False,
            quoting=csv.QUOTE_ALL,  # 全量加引号，尽量减少 Excel 自动识别为数字的概率
        )
        data = buf.getvalue().encode("utf-8-sig")  # 使用 utf-8-sig 兼容 Excel 打开
        return StreamingResponse(  # 返回流式响应
            io.BytesIO(data),  # bytes 流
            media_type="text/csv; charset=utf-8",  # MIME
            headers={"Content-Disposition": 'attachment; filename="result.csv"'},  # 下载文件名
        )

    if fmt == "xlsx":  # 导出 Excel
        bio = io.BytesIO()  # 二进制缓冲
        with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:  # 创建 ExcelWriter
            df.to_excel(writer, index=False, sheet_name="result")  # 写入 sheet
            # 将第一列（value）设置为“文本格式(@)”
            workbook = writer.book  # xlsxwriter workbook
            worksheet = writer.sheets["result"]  # 获取 worksheet
            text_fmt = workbook.add_format({"num_format": "@"})  # 文本格式
            worksheet.set_column(0, 0, 40, text_fmt)  # 设置 A 列宽度与格式
        bio.seek(0)  # 重置指针
        return StreamingResponse(  # 返回流式响应
            bio,  # bytes 流
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # MIME
            headers={"Content-Disposition": 'attachment; filename="result.xlsx"'},  # 下载文件名
        )

    _raise(400, "unsupported_format", "format 仅支持 csv 或 xlsx")  # 不支持的格式


@app.post("/api/export_raw")
async def api_export_raw(req: ExportRawRequest) -> StreamingResponse:
    """
    基于集合运算结果导出原始数据接口。
    
    功能说明：
    1. 获取集合运算的结果 ID 列表（例如第 6 步计算出的 83 个 patient_id）
    2. 根据指定的列名（默认为 patient_id），从原始文件中筛选出匹配的行
    3. 保留原始文件的所有列
    4. 将每个文件的筛选结果打包为 ZIP 文件返回
    """
    store.cleanup()  # 清理过期会话
    
    # 获取集合运算结果（ID 列表）
    try:
        result = store.get_result(req.session_id, req.result_id)  # 获取结果对象
    except KeyError:
        _raise(404, "result_not_found", "找不到结果，请重新计算")  # 结果不存在
    
    # 将结果值转换为集合，便于快速查找
    id_set = set(result.values)  # 结果 ID 集合
    
    # 创建 ID 顺序映射：将每个 ID 映射到它在结果列表中的位置（用于后续排序）
    # 说明：这样可以确保所有导出文件的行顺序与结果 ID 列表的顺序一致
    id_order_map = {v: idx for idx, v in enumerate(result.values)}  # ID -> 顺序位置
    
    # 为每个文件生成筛选后的 DataFrame
    filtered_files = []  # 存储 (filename, dataframe) 元组
    
    for file_id in req.file_ids:  # 遍历要导出的文件
        try:
            # 获取原始 DataFrame
            df = store.get_df(req.session_id, file_id)  # 获取 DataFrame
            # 获取文件信息（用于获取原始文件名）
            sf = store.get_file(req.session_id, file_id)  # 获取文件对象
        except KeyError:
            _raise(404, "file_not_found", f"文件 {file_id} 不存在，请重新上传")  # 文件不存在
        
        # 检查列是否存在
        if req.column_name not in df.columns:  # 列不存在
            _raise(400, "column_not_found", f"文件 {sf.filename} 中不存在列 {req.column_name}")  # 抛错
        
        # 筛选：保留 column_name 列的值在 id_set 中的所有行
        # 说明：对于字符串值，需要做 strip 处理（与集合运算逻辑保持一致）
        mask = df[req.column_name].apply(lambda v: (
            (v.strip() if isinstance(v, str) else v) in id_set
        ))  # 创建布尔掩码
        
        df_filtered = df[mask].copy()  # 筛选出匹配的行（保留所有列）
        
        # 按照结果 ID 列表的顺序对筛选后的行进行排序
        # 说明：创建一个排序键列，表示每行 ID 在结果列表中的位置
        def get_sort_key(v):
            # 对字符串做 strip 处理后查找顺序
            normalized_v = v.strip() if isinstance(v, str) else v
            # 返回该 ID 在结果列表中的位置，如果不存在则返回一个很大的数（理论上不会发生）
            return id_order_map.get(normalized_v, float('inf'))
        
        df_filtered['_sort_key'] = df_filtered[req.column_name].apply(get_sort_key)  # 添加排序键列
        df_filtered = df_filtered.sort_values('_sort_key')  # 按排序键排序
        df_filtered = df_filtered.drop('_sort_key', axis=1)  # 删除临时排序键列
        df_filtered = df_filtered.reset_index(drop=True)  # 重置索引，使行号从 0 开始连续
        
        # 生成导出文件名：原文件名_filtered.xlsx
        base_name, ext = os.path.splitext(sf.filename)  # 分离文件名和扩展名
        new_filename = f"{base_name}_filtered.xlsx"  # 新文件名
        
        filtered_files.append((new_filename, df_filtered))  # 记录
    
    # 如果只有一个文件，直接返回 Excel
    if len(filtered_files) == 1:  # 只有一个文件
        filename, df = filtered_files[0]  # 获取文件名和 DataFrame
        bio = io.BytesIO()  # 二进制缓冲
        with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:  # 创建 ExcelWriter
            df.to_excel(writer, index=False, sheet_name="filtered")  # 写入 sheet
        bio.seek(0)  # 重置指针
        return StreamingResponse(  # 返回流式响应
            bio,  # bytes 流
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # MIME
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},  # 下载文件名
        )
    
    # 多个文件：打包为 ZIP
    import zipfile
    zip_bio = io.BytesIO()  # ZIP 文件缓冲
    with zipfile.ZipFile(zip_bio, "w", zipfile.ZIP_DEFLATED) as zf:  # 创建 ZIP
        for filename, df in filtered_files:  # 遍历每个文件
            # 将 DataFrame 写入内存中的 Excel
            excel_bio = io.BytesIO()  # Excel 缓冲
            with pd.ExcelWriter(excel_bio, engine="xlsxwriter") as writer:  # 创建 ExcelWriter
                df.to_excel(writer, index=False, sheet_name="filtered")  # 写入 sheet
            excel_bio.seek(0)  # 重置指针
            # 添加到 ZIP
            zf.writestr(filename, excel_bio.read())  # 写入 ZIP
    
    zip_bio.seek(0)  # 重置指针
    return StreamingResponse(  # 返回流式响应
        zip_bio,  # bytes 流
        media_type="application/zip",  # MIME
        headers={"Content-Disposition": 'attachment; filename="filtered_data.zip"'},  # 下载文件名
    )


# 静态前端托管（Vanilla 前端）
static_dir = os.path.join(os.path.dirname(__file__), "static")  # 静态目录路径
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")  # 挂载为站点根


