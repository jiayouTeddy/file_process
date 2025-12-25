# 文件列值集合交集/差集在线工具（FastAPI + 原生前端）

## 功能概览（中文说明）

- 支持上传多个 **Excel（`.xlsx/.xls`）或 CSV（`.csv`）** 文件（只支持这两类）。
- 读取文件为 DataFrame，并展示：
  - 原始列名
  - 后端给出的“建议规范列名”（你可在网页上手动修改确认）
  - NA/空值位置（哪一行哪一列）
  - 数据预览（前 N 行）
- 列名规范化：你在网页上**手动确认/修改**列名映射后应用，后端会返回并展示**规范化后的列名**。
- 集合运算：针对多个文件中“相同列名”，对该列的**唯一值集合**做：
  - 交集（intersection）
  - 差集（difference，base - others）
  - 对称差集（symmetric_difference）
- 集合运算时的值清洗：对字符串值仅做 **去首尾空格（strip）**。
- 结果可导出为 CSV 或 XLSX。

## 快速启动

> 说明：此项目依赖 Python 3.10+（推荐 3.11/3.12）。

1) 创建并激活虚拟环境（示例）

```bash
python -m venv .venv
source .venv/bin/activate
```

2) 安装依赖

```bash
pip install -r requirements.txt
```

3) 启动服务（默认 http://127.0.0.1:8000）

```bash
uvicorn backend.app.main:app --reload
```

4) 打开网页

- 浏览器访问：`http://127.0.0.1:8000/`

## 使用流程（网页）

1. 上传多个 Excel/CSV 文件
2. 对 Excel 文件：为每个文件选择要解析的 sheet（CSV 不需要）
3. 点击解析后查看预览、NA 报告、列名建议
4. 在“列名规范化”区域手动确认/修改映射并应用
5. 在“集合运算”区域选择参与文件、共同列名、运算类型
6. 运行后预览结果并下载（CSV/XLSX）

## 重要约定

- NA 判定：Pandas `isna()` 判真（包含空单元格/NaN/None）；CSV 的空字符串默认也会被视作 NA（实现里做了常用兜底）。
- 行号：NA 报告中的 `row` 为 **1-based**（更贴近你在 Excel/表格里看到的行号）。


