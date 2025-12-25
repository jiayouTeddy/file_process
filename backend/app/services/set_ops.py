from __future__ import annotations

from functools import reduce
from typing import Iterable, List, Set

import pandas as pd


def _series_to_value_set(series: pd.Series, drop_na: bool) -> Set[object]:
    """
    将某一列（Series）转换为“唯一值集合”。

    你的约定：
    - 值清洗：仅对字符串做 strip（去首尾空格）
    - drop_na=True 时：NA 不参与集合运算
    """

    out: Set[object] = set()  # 输出集合
    for v in series.tolist():  # 转为 Python 列表后遍历（便于逐个处理）
        # 处理 NA：pandas 的 NA/NaN/None 等统一用 isna 判断
        if pd.isna(v):  # 若是 NA
            if drop_na:  # 若选择丢弃 NA
                continue  # 跳过
            out.add(None)  # 否则将 NA 统一用 None 表示
            continue  # 继续下一个值

        # 处理字符串：仅去首尾空格
        if isinstance(v, str):  # 若是字符串
            v2 = v.strip()  # 去首尾空格
            out.add(v2)  # 放入集合
        else:
            out.add(v)  # 非字符串原样放入集合

    return out  # 返回集合


def intersection(sets: List[Set[object]]) -> Set[object]:
    # 多集合交集：所有集合共同拥有的元素
    if not sets:  # 若为空
        return set()  # 返回空集
    return set.intersection(*sets)  # Python 内置交集


def difference(base: Set[object], others: Iterable[Set[object]]) -> Set[object]:
    # 差集：base - (others 的并集)
    other_union: Set[object] = set()  # 初始化并集
    for s in others:  # 遍历其它集合
        other_union |= s  # 做并集
    return base - other_union  # 返回差集


def symmetric_difference(sets: List[Set[object]]) -> Set[object]:
    # 多集合对称差集：出现于奇数个集合的元素
    if not sets:  # 若为空
        return set()  # 返回空集
    return reduce(lambda a, b: a ^ b, sets)  # 逐个做对称差


def compute_set_op(
    dfs: List[pd.DataFrame],
    column_name: str,
    op: str,
    drop_na: bool,
    base_index: int | None = None,
) -> List[object]:
    """
    对多个 DataFrame 的同名列执行集合运算，返回结果值列表（用于展示/导出）。
    """

    # 把每个 df 的该列转为 set
    sets: List[Set[object]] = []  # 每个文件的集合
    for df in dfs:  # 遍历 df
        if column_name not in df.columns:  # 列不存在
            raise KeyError(f"column_not_found: {column_name}")  # 抛错
        s = _series_to_value_set(df[column_name], drop_na=drop_na)  # 转 set
        sets.append(s)  # 记录

    # 根据 op 执行运算
    if op == "intersection":  # 交集
        res = intersection(sets)  # 计算交集
    elif op == "difference":  # 差集
        if base_index is None:  # 未指定 base
            raise ValueError("base_index_required_for_difference")  # 抛错
        base = sets[base_index]  # 取 base 集合
        others = [s for i, s in enumerate(sets) if i != base_index]  # 其它集合
        res = difference(base, others)  # 计算差集
    elif op == "symmetric_difference":  # 对称差集
        res = symmetric_difference(sets)  # 计算对称差
    else:
        raise ValueError("unsupported_op")  # 不支持的 op

    # 为了导出稳定性，这里做一个“可排序则排序，不可排序则转字符串排序”的处理
    try:
        values = sorted(res)  # 尝试直接排序（同类型、可比时可行）
    except Exception:
        values = sorted(list(res), key=lambda x: str(x))  # 否则按字符串排序

    return values  # 返回最终列表


