// 原生前端逻辑（Vanilla JS）。
// 说明：这里尽量用“清晰的状态对象 + 简单 DOM 操作”实现完整流程。
// 你的关键要求：列名规范化需要手动确认/修改；集合运算为“值集合运算”；值清洗仅 strip。

// -----------------------------
// 全局状态（尽量集中管理，便于调试）
// -----------------------------

const state = {
  // 当前会话 ID（后端 /api/upload 返回）
  sessionId: null,
  // 文件列表（后端返回的 files 数组，另外会补充解析/规范化后的信息）
  files: [],
  // 每个 file_id 的解析信息：columns_original / columns_suggestions / na_cells / preview_rows
  parsed: {},
  // 每个 file_id 的规范化后的列名（应用 normalize 后更新）
  normalizedColumns: {},
  // 最近一次集合运算结果
  lastResult: {
    resultId: null,
    count: 0,
    preview: [],
  },
};

// -----------------------------
// DOM 工具函数
// -----------------------------

function $(id) {
  // 通过 id 获取 DOM 元素
  return document.getElementById(id);
}

function escapeHtml(s) {
  // 简单 HTML 转义，避免把内容当 HTML 注入页面
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setStatus(id, text) {
  // 设置状态文本
  $(id).textContent = text || "";
}

// -----------------------------
// API 调用封装
// -----------------------------

async function apiUpload(files, sessionId) {
  // 调用 /api/upload 上传多个文件
  // 说明：
  // - sessionId 为空：第一次上传，后端创建新会话
  // - sessionId 不为空：追加上传到同一会话，避免“分两次上传导致只剩一个文件”
  const form = new FormData(); // 创建 multipart 表单
  if (sessionId) {
    form.append("session_id", sessionId); // 作为表单字段传给后端
  }
  for (const f of files) {
    form.append("files", f); // 后端参数名为 files
  }

  const resp = await fetch("/api/upload", {
    method: "POST",
    body: form,
  });

  const data = await resp.json();
  if (!resp.ok) {
    throw new Error(JSON.stringify(data));
  }
  return data;
}

async function apiParse(sessionId, fileId, sheetName) {
  // 调用 /api/parse 解析指定文件（Excel 需要 sheetName）
  const resp = await fetch("/api/parse", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      session_id: sessionId,
      file_id: fileId,
      sheet_name: sheetName || null,
    }),
  });
  const data = await resp.json();
  if (!resp.ok) {
    throw new Error(JSON.stringify(data));
  }
  return data;
}

async function apiNormalize(sessionId, fileId, renameMap) {
  // 调用 /api/normalize 应用列名映射
  const resp = await fetch("/api/normalize", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      session_id: sessionId,
      file_id: fileId,
      rename_map: renameMap,
    }),
  });
  const data = await resp.json();
  if (!resp.ok) {
    throw new Error(JSON.stringify(data));
  }
  return data;
}

async function apiSetOps(payload) {
  // 调用 /api/setops 执行集合运算
  const resp = await fetch("/api/setops", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (!resp.ok) {
    throw new Error(JSON.stringify(data));
  }
  return data;
}

async function apiExportRaw(payload) {
  // 调用 /api/export_raw 导出原始数据（基于结果 ID 筛选）
  const resp = await fetch("/api/export_raw", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  
  // 这个接口返回文件流（ZIP 或 Excel），不是 JSON
  if (!resp.ok) {
    // 如果出错，尝试读取 JSON 错误信息
    try {
      const errorData = await resp.json();
      throw new Error(JSON.stringify(errorData));
    } catch {
      throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    }
  }
  
  return resp; // 返回 Response 对象（供下载使用）
}

// -----------------------------
// UI 渲染：文件列表（选择 sheet + 解析）
// -----------------------------

function renderFilesArea() {
  // 渲染“文件解析区”
  const area = $("filesArea"); // 获取容器
  area.innerHTML = ""; // 清空旧内容

  if (!state.sessionId || state.files.length === 0) {
    area.innerHTML = `<div class="hint">请先上传文件。</div>`;
    return;
  }

  for (const file of state.files) {
    // 为每个文件创建一个卡片行
    const div = document.createElement("div");
    div.className = "card";

    // 生成 Excel sheet 下拉（如果是 excel）
    let sheetHtml = "";
    if (file.file_type === "excel") {
      const opts = (file.sheet_names || []).map((s) => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`);
      sheetHtml = `
        <label>sheet：</label>
        <select id="sheet_${file.file_id}">
          ${opts.join("")}
        </select>
      `;
    } else {
      sheetHtml = `<span class="hint">CSV 无需选择 sheet</span>`;
    }

    // 解析按钮
    const parsedMark = state.parsed[file.file_id] ? "（已解析）" : "";

    div.innerHTML = `
      <div class="row">
        <strong>${escapeHtml(file.filename)}</strong>
        <span class="hint">type: ${escapeHtml(file.file_type)} ${parsedMark}</span>
      </div>
      <div class="row">
        ${sheetHtml}
        <button id="btnParse_${file.file_id}">解析</button>
      </div>
      <div id="parseStatus_${file.file_id}" class="status"></div>
    `;

    area.appendChild(div);

    // 绑定解析按钮事件
    const btn = document.getElementById(`btnParse_${file.file_id}`);
    btn.addEventListener("click", async () => {
      try {
        setStatus(`parseStatus_${file.file_id}`, "解析中...");
        const sheetName = file.file_type === "excel" ? document.getElementById(`sheet_${file.file_id}`).value : null;
        const data = await apiParse(state.sessionId, file.file_id, sheetName);
        state.parsed[file.file_id] = data;
        // 默认规范化列名还没应用，因此 normalizedColumns 先不写；后续 apply normalize 后写
        setStatus(`parseStatus_${file.file_id}`, "解析完成：已生成预览、NA 报告与列名建议。");
        // 刷新：预览、NA、规范化文件下拉、集合运算文件 checkbox
        renderPreviewAndNa(file.file_id);
        renderNormalizeFileSelect();
        renderSetOpsFiles();
        renderExportRawFilesCheckboxes();
      } catch (e) {
        setStatus(`parseStatus_${file.file_id}`, `解析失败：${e.message}`);
      }
    });
  }
}

// -----------------------------
// UI 渲染：预览与 NA
// -----------------------------

function renderPreviewAndNa(fileId) {
  // 渲染“预览表格”与“NA 列表”
  const parsed = state.parsed[fileId];
  if (!parsed) {
    return;
  }

  // 渲染预览表格
  const rows = parsed.preview_rows || [];
  const cols = parsed.columns_original || [];
  const tableHtml = renderTable(cols, rows);
  $("previewArea").innerHTML = tableHtml;

  // 渲染 NA 列表（文本形式，便于快速查看）
  const na = parsed.na_cells || [];
  if (na.length === 0) {
    $("naArea").textContent = "未发现 NA/空值。";
  } else {
    // NA 可能非常多：为了不把页面无限拉长，这里只展示前 N 条，并显示总数
    const maxShow = 200; // 前端展示上限（后端仍会返回最多 K 条；这里进一步限制 UI 展示）
    const head = na.slice(0, maxShow);
    const lines = head.map((x) => `row=${x.row}, col=${x.col}`);
    const prefix = `NA 总数（后端返回）：${na.length}\n展示前 ${head.length} 条：\n`;
    const suffix = na.length > head.length ? `\n...\n（已截断展示，避免页面过长）` : "";
    $("naArea").textContent = prefix + lines.join("\n") + suffix;
  }
}

function renderTable(columns, rows) {
  // 把 columns + rows 渲染成 HTML 表格
  const thead = `<tr>${columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr>`;
  const tbody = rows
    .map((r) => {
      const tds = columns.map((c) => `<td>${escapeHtml(r[c] ?? "")}</td>`).join("");
      return `<tr>${tds}</tr>`;
    })
    .join("");
  return `<table><thead>${thead}</thead><tbody>${tbody}</tbody></table>`;
}

// -----------------------------
// UI 渲染：列名规范化（手动确认/修改）
// -----------------------------

function renderNormalizeFileSelect() {
  // 渲染“选择要规范化的文件”下拉
  const sel = $("normalizeFileSelect");
  sel.innerHTML = "";

  // 只把“已解析”的文件加入下拉（因为规范化依赖 parse 后的列名）
  const parsedFileIds = Object.keys(state.parsed);
  if (parsedFileIds.length === 0) {
    sel.innerHTML = `<option value="">请先解析至少一个文件</option>`;
    return;
  }

  for (const file of state.files) {
    if (!state.parsed[file.file_id]) continue;
    const opt = document.createElement("option");
    opt.value = file.file_id;
    opt.textContent = `${file.filename}`;
    sel.appendChild(opt);
  }
}

function renderNormalizeTable(fileId) {
  // 渲染“列名建议表”（原列名 + 建议列名输入框）
  const parsed = state.parsed[fileId];
  if (!parsed) {
    $("normalizeTableArea").innerHTML = "";
    return;
  }

  const cols = parsed.columns_original || [];
  const sugg = parsed.columns_suggestions || [];

  // 构造表格 HTML
  const head = `<tr><th>原列名</th><th>建议规范名（可编辑）</th></tr>`;
  const body = cols
    .map((c, idx) => {
      const s = sugg[idx] ?? "";
      return `
        <tr>
          <td>${escapeHtml(c)}</td>
          <td>
            <input
              type="text"
              class="renameInput"
              data-old="${escapeHtml(c)}"
              value="${escapeHtml(s)}"
            />
          </td>
        </tr>
      `;
    })
    .join("");

  $("normalizeTableArea").innerHTML = `<table><thead>${head}</thead><tbody>${body}</tbody></table>`;
}

function collectRenameMapFromTable() {
  // 从规范化表格收集 rename_map（原列名 -> 新列名）
  const inputs = document.querySelectorAll(".renameInput");
  const map = {};
  for (const inp of inputs) {
    const oldName = inp.getAttribute("data-old");
    const newName = inp.value;
    map[oldName] = newName;
  }
  return map;
}

// -----------------------------
// UI 渲染：集合运算
// -----------------------------

function renderSetOpsFiles() {
  // 渲染"参与运算文件"复选框
  const wrap = $("setopsFilesCheckboxes");
  wrap.innerHTML = "";

  // 只显示已解析的文件（否则无法运算）
  const parsedFileIds = state.files.filter((f) => state.parsed[f.file_id]).map((f) => f.file_id);
  if (parsedFileIds.length === 0) {
    wrap.innerHTML = `<span class="hint">请先解析文件。</span>`;
    return;
  }

  for (const file of state.files) {
    if (!state.parsed[file.file_id]) continue;
    const label = document.createElement("label");
    label.className = "inline";
    label.innerHTML = `
      <input type="checkbox" class="setopsFileCk" value="${escapeHtml(file.file_id)}" />
      ${escapeHtml(file.filename)}
    `;
    wrap.appendChild(label);
  }

  // 同步 base 下拉
  renderBaseSelect();
  // 同步导出原始数据的文件选择框
  renderExportRawFilesCheckboxes();
}

function renderExportRawFilesCheckboxes() {
  // 渲染"导出原始数据"的文件复选框
  const wrap = $("exportRawFilesCheckboxes");
  wrap.innerHTML = "";

  // 只显示已解析的文件
  const parsedFileIds = state.files.filter((f) => state.parsed[f.file_id]).map((f) => f.file_id);
  if (parsedFileIds.length === 0) {
    wrap.innerHTML = `<span class="hint">请先解析文件。</span>`;
    return;
  }

  for (const file of state.files) {
    if (!state.parsed[file.file_id]) continue;
    const label = document.createElement("label");
    label.className = "inline";
    label.innerHTML = `
      <input type="checkbox" class="exportRawFileCk" value="${escapeHtml(file.file_id)}" />
      ${escapeHtml(file.filename)}
    `;
    wrap.appendChild(label);
  }
}

function getSelectedFileIdsForSetOps() {
  // 获取勾选的 file_ids
  const cks = document.querySelectorAll(".setopsFileCk");
  const ids = [];
  for (const ck of cks) {
    if (ck.checked) ids.push(ck.value);
  }
  return ids;
}

function getSelectedFileIdsForExportRaw() {
  // 获取导出原始数据勾选的 file_ids
  const cks = document.querySelectorAll(".exportRawFileCk");
  const ids = [];
  for (const ck of cks) {
    if (ck.checked) ids.push(ck.value);
  }
  return ids;
}

function renderBaseSelect() {
  // 渲染 difference 的 base 文件下拉（从“已解析文件”里选）
  const sel = $("setopsBaseSelect");
  sel.innerHTML = "";

  for (const file of state.files) {
    if (!state.parsed[file.file_id]) continue;
    const opt = document.createElement("option");
    opt.value = file.file_id;
    opt.textContent = file.filename;
    sel.appendChild(opt);
  }
}

function computeCommonColumns(fileIds) {
  // 计算多个文件的共同列名（以“规范化后列名优先”，否则用原始列名）
  // 说明：如果文件已经 normalize，则以 normalizedColumns[fileId] 为准；否则退回到 parse 的 columns_original。
  const colSets = [];
  for (const fid of fileIds) {
    const cols = state.normalizedColumns[fid] || state.parsed[fid]?.columns_original || [];
    colSets.push(new Set(cols));
  }
  if (colSets.length === 0) return [];
  let common = colSets[0];
  for (let i = 1; i < colSets.length; i++) {
    const next = colSets[i];
    common = new Set([...common].filter((x) => next.has(x)));
  }
  return [...common].sort();
}

function renderCommonColumnsSelect() {
  // 刷新共同列名下拉
  const ids = getSelectedFileIdsForSetOps();
  const sel = $("setopsColumnSelect");
  sel.innerHTML = "";

  if (ids.length < 2) {
    sel.innerHTML = `<option value="">请先选择至少 2 个文件</option>`;
    return;
  }

  const commons = computeCommonColumns(ids);
  if (commons.length === 0) {
    sel.innerHTML = `<option value="">没有共同列名（请先规范化列名）</option>`;
    return;
  }

  for (const c of commons) {
    const opt = document.createElement("option");
    opt.value = c;
    opt.textContent = c;
    sel.appendChild(opt);
  }
}

// -----------------------------
// 结果区渲染与下载
// -----------------------------

function renderResult() {
  // 渲染结果预览与统计信息
  if (!state.lastResult.resultId) {
    $("resultSummary").textContent = "暂无结果，请先运行集合运算。";
    $("resultPreview").textContent = "";
    return;
  }
  $("resultSummary").textContent = `result_id=${state.lastResult.resultId}\ncount=${state.lastResult.count}`;
  $("resultPreview").textContent = (state.lastResult.preview || []).map((v) => String(v)).join("\n");
}

function downloadResult(format) {
  // 触发下载（直接打开导出 URL）
  if (!state.lastResult.resultId || !state.sessionId) {
    alert("暂无可下载结果，请先运行集合运算。");
    return;
  }
  const url = `/api/export?session_id=${encodeURIComponent(state.sessionId)}&result_id=${encodeURIComponent(
    state.lastResult.resultId
  )}&format=${encodeURIComponent(format)}`;
  window.open(url, "_blank");
}

// -----------------------------
// 事件绑定：上传 / 规范化 / 集合运算
// -----------------------------

async function onUpload() {
  // 点击“上传”按钮事件
  const input = $("fileInput");
  const files = input.files;
  if (!files || files.length === 0) {
    alert("请选择至少一个文件");
    return;
  }

  try {
    const oldSessionId = state.sessionId; // 保存旧会话（用于判断是否追加上传/会话是否变化）
    const isAppend = Boolean(oldSessionId); // 是否为追加上传

    setStatus("uploadStatus", isAppend ? "追加上传中..." : "上传中...");
    const data = await apiUpload(files, oldSessionId);

    const newSessionId = data.session_id; // 后端返回的 session_id
    const newFiles = data.files || []; // 本次上传新增的文件列表（后端只返回新增部分）

    if (!isAppend || !oldSessionId) {
      // 第一次上传：初始化会话与状态
      state.sessionId = newSessionId;
      state.files = newFiles;
      state.parsed = {};
      state.normalizedColumns = {};
      state.lastResult = { resultId: null, count: 0, preview: [] };
      setStatus("uploadStatus", `上传成功：session_id=${state.sessionId}\n共 ${state.files.length} 个文件。`);
    } else if (newSessionId !== oldSessionId) {
      // 追加上传但会话发生变化：通常是会话过期/不存在，后端降级为创建新会话
      // 为避免后续操作引用旧 file_id 失败，这里直接清空旧状态并提示用户重新解析
      state.sessionId = newSessionId;
      state.files = newFiles;
      state.parsed = {};
      state.normalizedColumns = {};
      state.lastResult = { resultId: null, count: 0, preview: [] };
      setStatus(
        "uploadStatus",
        `原会话已失效，已创建新会话：session_id=${state.sessionId}\n当前仅保留本次上传的 ${state.files.length} 个文件，请重新解析。`
      );
    } else {
      // 正常追加上传：合并文件列表，保持已解析/已规范化状态不变
      state.sessionId = newSessionId;
      state.files = [...state.files, ...newFiles];
      setStatus(
        "uploadStatus",
        `追加上传成功：session_id=${state.sessionId}\n新增 ${newFiles.length} 个文件，当前共 ${state.files.length} 个文件。`
      );
    }

    // 清空 input，避免某些浏览器选择同一文件时不触发 change
    input.value = "";

    // 渲染文件解析区与相关 UI
    renderFilesArea();
    renderNormalizeFileSelect();
    renderSetOpsFiles();
    renderCommonColumnsSelect();
    renderResult();
    renderExportRawFilesCheckboxes();
  } catch (e) {
    setStatus("uploadStatus", `上传失败：${e.message}`);
  }
}

function onLoadNormalizeTable() {
  // 点击“加载列名建议”按钮事件
  const fileId = $("normalizeFileSelect").value;
  if (!fileId) return;
  setStatus("normalizeStatus", "");
  renderNormalizeTable(fileId);
}

async function onApplyNormalize() {
  // 点击“应用规范化列名”按钮事件
  const fileId = $("normalizeFileSelect").value;
  if (!fileId) {
    alert("请先选择要规范化的文件");
    return;
  }
  if (!state.sessionId) {
    alert("请先上传并解析文件");
    return;
  }

  try {
    setStatus("normalizeStatus", "应用中...");
    const renameMap = collectRenameMapFromTable();
    const data = await apiNormalize(state.sessionId, fileId, renameMap);
    // 保存规范化后的列名
    state.normalizedColumns[fileId] = data.columns_normalized || [];
    setStatus("normalizeStatus", `规范化完成。规范化后的列名：\n${(data.columns_normalized || []).join(", ")}`);
    // 刷新共同列名（因为列名可能变化）
    renderCommonColumnsSelect();
  } catch (e) {
    setStatus("normalizeStatus", `规范化失败：${e.message}`);
  }
}

async function onRunSetOps() {
  // 点击"运行集合运算"按钮事件
  const fileIds = getSelectedFileIdsForSetOps();
  const columnName = $("setopsColumnSelect").value;
  const op = $("setopsOpSelect").value;
  const baseFileId = $("setopsBaseSelect").value;
  const dropNa = $("setopsDropNa").checked;

  if (!state.sessionId) {
    alert("请先上传文件");
    return;
  }
  if (fileIds.length < 2) {
    alert("请至少选择 2 个文件");
    return;
  }
  if (!columnName) {
    alert("请选择一个共同列名");
    return;
  }

  try {
    setStatus("setopsStatus", "运算中...");
    const payload = {
      session_id: state.sessionId,
      file_ids: fileIds,
      column_name: columnName,
      op,
      base_file_id: op === "difference" ? baseFileId : null,
      drop_na: dropNa,
    };
    const data = await apiSetOps(payload);
    state.lastResult = {
      resultId: data.result_id,
      count: data.count,
      preview: data.values_preview || [],
    };
    setStatus("setopsStatus", "运算完成。请在下方查看结果并下载。");
    renderResult();
  } catch (e) {
    setStatus("setopsStatus", `运算失败：${e.message}`);
  }
}

async function onExportRaw() {
  // 点击"导出原始数据"按钮事件
  const fileIds = getSelectedFileIdsForExportRaw();
  const columnName = $("exportRawColumnName").value.trim();

  if (!state.sessionId) {
    alert("请先上传文件");
    return;
  }
  if (!state.lastResult.resultId) {
    alert("请先运行集合运算，生成结果 ID 列表");
    return;
  }
  if (fileIds.length === 0) {
    alert("请至少选择一个要导出的文件");
    return;
  }
  if (!columnName) {
    alert("请输入筛选列名（例如：patient_id）");
    return;
  }

  try {
    setStatus("exportRawStatus", "导出中，请稍候...");
    const payload = {
      session_id: state.sessionId,
      result_id: state.lastResult.resultId,
      file_ids: fileIds,
      column_name: columnName,
    };
    const resp = await apiExportRaw(payload);

    // 获取文件名（从 Content-Disposition 头中提取）
    const disposition = resp.headers.get("Content-Disposition");
    let filename = "filtered_data.zip"; // 默认文件名
    if (disposition) {
      const match = disposition.match(/filename="?(.+?)"?$/);
      if (match) filename = match[1];
    }

    // 将响应转为 Blob 并触发下载
    const blob = await resp.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);

    setStatus("exportRawStatus", `导出成功：${filename}`);
  } catch (e) {
    setStatus("exportRawStatus", `导出失败：${e.message}`);
  }
}

// -----------------------------
// 页面初始化：绑定事件
// -----------------------------

function init() {
  // 绑定上传按钮
  $("btnUpload").addEventListener("click", onUpload);
  // 绑定加载列名建议按钮
  $("btnLoadNormalize").addEventListener("click", onLoadNormalizeTable);
  // 绑定应用规范化按钮
  $("btnApplyNormalize").addEventListener("click", onApplyNormalize);
  // 绑定刷新共同列名按钮
  $("btnRefreshCommonCols").addEventListener("click", renderCommonColumnsSelect);
  // 绑定集合运算按钮
  $("btnRunSetOps").addEventListener("click", onRunSetOps);
  // 绑定下载按钮
  $("btnDownloadCsv").addEventListener("click", () => downloadResult("csv"));
  $("btnDownloadXlsx").addEventListener("click", () => downloadResult("xlsx"));
  // 绑定导出原始数据按钮
  $("btnExportRaw").addEventListener("click", onExportRaw);

  // 初始渲染提示
  renderFilesArea();
  renderNormalizeFileSelect();
  renderSetOpsFiles();
  renderCommonColumnsSelect();
  renderResult();
}

// DOM ready 后执行初始化
document.addEventListener("DOMContentLoaded", init);


