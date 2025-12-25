from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import pandas as pd


@dataclass
class StoredFile:
    # 文件名（用于前端展示）
    filename: str
    # 文件类型：csv 或 excel
    file_type: str
    # 原始文件二进制内容（为了简化实现，先用内存缓存；后续可改临时文件）
    content: bytes
    # Excel 的 sheet 名列表（CSV 为空）
    sheet_names: Optional[list[str]] = None
    # 最近一次解析时选择的 sheet（CSV 为 None）
    selected_sheet: Optional[str] = None


@dataclass
class StoredResult:
    # 结果值列表（集合运算输出）
    values: list[object] = field(default_factory=list)
    # 创建时间（用于 TTL 清理）
    created_at: float = field(default_factory=lambda: time.time())


@dataclass
class SessionData:
    # 会话创建/最后访问时间（用于 TTL 清理）
    touched_at: float = field(default_factory=lambda: time.time())
    # 上传的文件缓存：file_id -> StoredFile
    files: Dict[str, StoredFile] = field(default_factory=dict)
    # 解析后的 DataFrame 缓存：file_id -> DataFrame
    dfs: Dict[str, pd.DataFrame] = field(default_factory=dict)
    # 集合运算结果缓存：result_id -> StoredResult
    results: Dict[str, StoredResult] = field(default_factory=dict)


class Store:
    """
    一个极简的内存缓存 Store。

    说明（中文逐行注释要点）：
    - 通过 session_id 将同一批上传与后续 parse/normalize/setops 串起来
    - 通过 TTL 定期清理过期会话，避免内存无限增长
    - 通过锁保证并发请求时的线程安全（uvicorn 默认多线程/异步环境下仍建议保护共享字典）
    """

    def __init__(
        self,
        ttl_seconds: int = 30 * 60,
        max_files_per_session: int = 20,
        max_file_bytes: int = 20 * 1024 * 1024,
        max_preview_rows: int = 30,
        max_na_cells: int = 5000,
        max_export_values: int = 2_000_000,
    ) -> None:
        # 会话 TTL（秒）
        self.ttl_seconds = ttl_seconds
        # 单会话最多文件数限制
        self.max_files_per_session = max_files_per_session
        # 单文件最大字节数限制
        self.max_file_bytes = max_file_bytes
        # 预览最多行数（后端解析时截断）
        self.max_preview_rows = max_preview_rows
        # NA 位置返回最多条数（避免前端渲染过多）
        self.max_na_cells = max_na_cells
        # 导出结果最大条数（避免导出过大拖垮内存/CPU）
        self.max_export_values = max_export_values

        # 内存中的会话字典：session_id -> SessionData
        self._sessions: Dict[str, SessionData] = {}
        # 互斥锁：保护 _sessions 及其内部结构
        self._lock = threading.Lock()

    def _now(self) -> float:
        # 返回当前时间戳（秒）
        return time.time()

    def _new_id(self) -> str:
        # 生成一个 UUID 字符串作为 ID
        return str(uuid.uuid4())

    def cleanup(self) -> None:
        # 清理过期会话（按 touched_at 判断）
        now = self._now()  # 获取当前时间
        with self._lock:  # 加锁，保证遍历与删除安全
            expired = []  # 用于存放过期 session_id
            for sid, sess in self._sessions.items():  # 遍历所有会话
                if now - sess.touched_at > self.ttl_seconds:  # 若超过 TTL
                    expired.append(sid)  # 记录为过期
            for sid in expired:  # 逐个删除过期会话
                self._sessions.pop(sid, None)  # 删除（不存在也不报错）

    def create_session(self) -> str:
        # 创建新会话并返回 session_id
        sid = self._new_id()  # 生成会话 ID
        with self._lock:  # 加锁写入
            self._sessions[sid] = SessionData()  # 初始化会话数据
        return sid  # 返回会话 ID

    def touch_session(self, session_id: str) -> None:
        # 更新会话触达时间（用于 TTL）
        with self._lock:  # 加锁读写
            sess = self._sessions.get(session_id)  # 获取会话
            if sess is not None:  # 若存在
                sess.touched_at = self._now()  # 更新最后访问时间

    def get_session(self, session_id: str) -> SessionData:
        # 获取会话数据；若不存在则抛出 KeyError
        with self._lock:  # 加锁读取
            sess = self._sessions.get(session_id)  # 获取会话
            if sess is None:  # 若不存在
                raise KeyError("session_not_found")  # 抛出错误
            sess.touched_at = self._now()  # 更新触达时间
            return sess  # 返回会话引用（同一进程内使用）

    def add_file(self, session_id: str, filename: str, file_type: str, content: bytes) -> str:
        # 往指定会话添加一个文件缓存，并返回 file_id
        if len(content) > self.max_file_bytes:  # 校验文件大小
            raise ValueError("file_too_large")  # 抛出错误
        sess = self.get_session(session_id)  # 获取会话（同时会 touch）
        with self._lock:  # 加锁写入会话内部
            if len(sess.files) >= self.max_files_per_session:  # 校验文件数量
                raise ValueError("too_many_files")  # 抛出错误
            file_id = self._new_id()  # 生成文件 ID
            sess.files[file_id] = StoredFile(  # 写入文件缓存
                filename=filename,
                file_type=file_type,
                content=content,
            )
            return file_id  # 返回文件 ID

    def set_excel_sheet_names(self, session_id: str, file_id: str, sheet_names: list[str]) -> None:
        # 写入 Excel 的 sheet 列表
        sess = self.get_session(session_id)  # 获取会话
        with self._lock:  # 加锁写入
            sf = sess.files.get(file_id)  # 获取文件对象
            if sf is None:  # 若不存在
                raise KeyError("file_not_found")  # 抛出错误
            sf.sheet_names = sheet_names  # 设置 sheet 名列表

    def get_file(self, session_id: str, file_id: str) -> StoredFile:
        # 获取会话中的文件对象
        sess = self.get_session(session_id)  # 获取会话
        sf = sess.files.get(file_id)  # 获取文件
        if sf is None:  # 若不存在
            raise KeyError("file_not_found")  # 抛出错误
        return sf  # 返回文件

    def put_df(self, session_id: str, file_id: str, df: pd.DataFrame, selected_sheet: Optional[str]) -> None:
        # 缓存解析后的 DataFrame（覆盖旧值）
        sess = self.get_session(session_id)  # 获取会话
        with self._lock:  # 加锁写入
            sess.dfs[file_id] = df  # 缓存 DataFrame
            sf = sess.files.get(file_id)  # 获取对应文件
            if sf is not None:  # 若存在
                sf.selected_sheet = selected_sheet  # 记录选择的 sheet

    def get_df(self, session_id: str, file_id: str) -> pd.DataFrame:
        # 获取已解析的 DataFrame
        sess = self.get_session(session_id)  # 获取会话
        df = sess.dfs.get(file_id)  # 获取 DataFrame
        if df is None:  # 若不存在
            raise KeyError("df_not_found")  # 抛出错误
        return df  # 返回 DataFrame

    def put_result(self, session_id: str, values: list[object]) -> str:
        # 缓存集合运算结果，并返回 result_id
        sess = self.get_session(session_id)  # 获取会话
        if len(values) > self.max_export_values:  # 导出数量上限保护
            raise ValueError("result_too_large")  # 抛出错误
        rid = self._new_id()  # 生成结果 ID
        with self._lock:  # 加锁写入
            sess.results[rid] = StoredResult(values=values)  # 写入结果
        return rid  # 返回结果 ID

    def get_result(self, session_id: str, result_id: str) -> StoredResult:
        # 获取结果对象
        sess = self.get_session(session_id)  # 获取会话
        res = sess.results.get(result_id)  # 获取结果
        if res is None:  # 若不存在
            raise KeyError("result_not_found")  # 抛出错误
        return res  # 返回结果

    def get_common_columns(self, session_id: str, file_ids: list[str]) -> Tuple[set[str], Dict[str, list[str]]]:
        # 计算多个文件 DataFrame 的共同列名（交集），同时返回各文件列名列表用于调试/展示
        cols_by_file: Dict[str, list[str]] = {}  # 每个文件的列名
        common: Optional[set[str]] = None  # 共同列名集合
        for fid in file_ids:  # 遍历文件
            df = self.get_df(session_id, fid)  # 获取 DataFrame
            cols = [str(c) for c in list(df.columns)]  # 转为字符串列名
            cols_by_file[fid] = cols  # 记录
            if common is None:  # 第一个文件初始化 common
                common = set(cols)  # 以第一个文件列为初始集合
            else:  # 后续文件取交集
                common &= set(cols)  # 更新交集
        return (common or set(), cols_by_file)  # 返回共同列集合与明细


# 创建一个全局 Store 实例（简单起见；生产环境可用依赖注入/外部缓存）
store = Store()


