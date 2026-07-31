const API_BASE = "/api/v1";
const DEFAULT_EXCLUDES = [
  ".git/**",
  "__pycache__/**",
  "*.tmp",
  "*.part",
  "Thumbs.db",
  ".DS_Store",
];
const ACTIVE_STATUSES = new Set(["queued", "running", "cancelling"]);
const TERMINAL_EVENT_STATUSES = {
  "operation.completed": "completed",
  "operation.failed": "failed",
  "operation.cancelled": "cancelled",
};
const SCHEDULE_PRESETS = {
  manual: "",
  hourly: "0 * * * *",
  "daily-2": "0 2 * * *",
  "daily-23": "0 23 * * *",
  "weekly-1": "0 2 * * 1",
};
const STORAGE_PROBE_IDLE_MESSAGE = "保存任务前，可验证写入、读取与删除权限。";
const STORAGE_CONFIG_FIELD_SELECTOR =
  'input[name="storage_kind"], input[name^="storage."]';
const STORAGE_DIAGNOSTIC_FACTS = [
  ["kind", "诊断类型"],
  ["problem_code", "问题代码"],
  ["provider_code", "服务商错误码"],
  ["http_status", "HTTP 状态"],
  ["request_id", "请求 ID"],
  ["endpoint", "Endpoint"],
  ["bucket", "Bucket"],
  ["region", "Region"],
  ["operation", "失败操作"],
];
const VIEW_META = {
  dashboard: {
    eyebrow: "CONTROL ROOM",
    title: "备份概览",
    subtitle: "所有任务、运行状态与系统健康一目了然。",
  },
  tasks: {
    eyebrow: "BACKUP PLANS",
    title: "备份任务",
    subtitle: "配置来源、目标、计划与生命周期策略。",
  },
  operations: {
    eyebrow: "ACTIVITY",
    title: "调度与日志",
    subtitle: "实时跟踪备份、还原、差分计算与完整性巡检。",
  },
  snapshots: {
    eyebrow: "ARCHIVES",
    title: "快照与差分还原",
    subtitle: "浏览 Base 与 xdelta3 差分版本，按文件精确恢复数据。",
  },
  credentials: {
    eyebrow: "SECURE VAULT",
    title: "存储与密钥",
    subtitle: "通过系统 Keyring 安全管理对象存储访问密钥。",
  },
  system: {
    eyebrow: "DIAGNOSTICS",
    title: "系统状态",
    subtitle: "检查服务、调度器与外部工具是否就绪。",
  },
};

const state = {
  view: "dashboard",
  tasks: [],
  operations: [],
  snapshots: [],
  credentials: [],
  system: null,
  snapshotTaskId: "",
  selectedSnapshotId: "",
  manifest: null,
  selectedPaths: new Set(),
  taskFilter: { query: "", status: "all" },
  operationFilter: "all",
  ws: null,
  wsState: "connecting",
  reconnectAttempt: 0,
  reconnectTimer: null,
  confirmAction: null,
  resyncTimer: null,
  authPromise: null,
  authResolve: null,
  dismissedOperationId: "",
  storageProbeRevision: 0,
  storageProbePending: false,
  storageProbeMode: "",
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

class ApiError extends Error {
  constructor(message, status = 0, payload = null, fieldErrors = []) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.payload = payload;
    this.fieldErrors = fieldErrors;
  }
}

function element(tag, className = "", text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

function append(parent, ...children) {
  for (const child of children) {
    if (child !== null && child !== undefined) parent.append(child);
  }
  return parent;
}

function clear(node) {
  if (node) node.replaceChildren();
}

function unwrapList(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.data)) return payload.data;
  return [];
}

function numberOr(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function truncate(value, max = 32) {
  const text = String(value ?? "");
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function formatBytes(value) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes < 0) return "—";
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const amount = bytes / 1024 ** index;
  const digits = amount >= 100 || index === 0 ? 0 : amount >= 10 ? 1 : 2;
  return `${amount.toFixed(digits)} ${units[index]}`;
}

function formatDate(value, includeTime = true) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    ...(includeTime ? { hour: "2-digit", minute: "2-digit", second: "2-digit" } : {}),
    hour12: false,
  }).format(date);
}

function formatRelative(value) {
  if (!value) return "从未运行";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const delta = date.getTime() - Date.now();
  const abs = Math.abs(delta);
  const formatter = new Intl.RelativeTimeFormat("zh-CN", { numeric: "auto" });
  if (abs < 60_000) return formatter.format(Math.round(delta / 1000), "second");
  if (abs < 3_600_000) return formatter.format(Math.round(delta / 60_000), "minute");
  if (abs < 86_400_000) return formatter.format(Math.round(delta / 3_600_000), "hour");
  return formatter.format(Math.round(delta / 86_400_000), "day");
}

function formatMtimeNs(value) {
  if (value === null || value === undefined) return "—";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "—";
  return formatDate(new Date(numeric / 1_000_000).toISOString());
}

function formatDuration(startValue, endValue) {
  if (!startValue) return "—";
  const start = new Date(startValue).getTime();
  const end = endValue ? new Date(endValue).getTime() : Date.now();
  if (!Number.isFinite(start) || !Number.isFinite(end)) return "—";
  const seconds = Math.max(0, Math.round((end - start) / 1000));
  if (seconds < 60) return `${seconds} 秒`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} 分 ${seconds % 60} 秒`;
  return `${Math.floor(seconds / 3600)} 小时 ${Math.floor((seconds % 3600) / 60)} 分`;
}

function taskById(id) {
  return state.tasks.find((task) => task.id === id);
}

function snapshotById(id) {
  return state.snapshots.find((snapshot) => snapshot.id === id);
}

function operationStatusLabel(status) {
  return {
    queued: "排队中",
    running: "进行中",
    cancelling: "正在取消",
    completed: "已完成",
    succeeded: "已完成",
    failed: "失败",
    cancelled: "已取消",
  }[String(status ?? "").toLowerCase()] ?? status ?? "未知";
}

function operationKindLabel(kind) {
  return {
    backup: "备份",
    restore: "还原",
    scrub: "巡检",
    prune: "清理",
  }[kind] ?? kind ?? "任务";
}

function phaseLabel(phase) {
  return {
    queued: "等待执行",
    scanning: "扫描文件",
    hashing: "计算差异",
    packing: "打包归档",
    compressing: "压缩数据",
    uploading: "上传分片",
    downloading: "下载归档",
    extracting: "提取文件",
    verifying: "校验完整性",
    finalizing: "写入索引",
    complete: "处理完成",
    completed: "处理完成",
    cancelled: "已取消",
  }[phase] ?? phase ?? "准备中";
}

function statusBadge(status, label) {
  const normalized = String(status ?? "neutral").toLowerCase();
  const badge = element("span", `status-badge ${normalized}`, label ?? operationStatusLabel(normalized));
  return badge;
}

function setButtonBusy(button, busy, busyText = "处理中…") {
  if (!button) return;
  if (busy) {
    button.dataset.originalText = button.textContent;
    button.disabled = true;
    button.textContent = busyText;
  } else {
    button.disabled = false;
    if (button.dataset.originalText) {
      button.textContent = button.dataset.originalText;
      delete button.dataset.originalText;
    }
  }
}

function extractApiError(payload, responseStatus) {
  const fieldErrors = [];
  if (Array.isArray(payload?.detail)) {
    for (const issue of payload.detail) {
      const path = Array.isArray(issue.loc)
        ? issue.loc.filter((part) => part !== "body").join(".")
        : "";
      fieldErrors.push({ path, message: issue.msg ?? "字段无效" });
    }
  }
  const problemFieldErrors = payload?.field_errors ?? payload?.details?.field_errors;
  if (Array.isArray(problemFieldErrors)) {
    for (const issue of problemFieldErrors) {
      fieldErrors.push({
        path: Array.isArray(issue.path) ? issue.path.join(".") : String(issue.path ?? ""),
        message: issue.message ?? issue.msg ?? "字段无效",
      });
    }
  }

  const message =
    payload?.message ??
    payload?.error?.message ??
    (typeof payload?.detail === "string" ? payload.detail : "") ??
    payload?.title ??
    `请求失败（${responseStatus}）`;
  return new ApiError(message || `请求失败（${responseStatus}）`, responseStatus, payload, fieldErrors);
}

async function api(path, options = {}) {
  const {
    timeout: timeoutMs = 25_000,
    _authRetried = false,
    ...requestOptions
  } = options;
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(requestOptions.headers ?? {});
  if (requestOptions.body !== undefined && !(requestOptions.body instanceof FormData)) {
    headers.set("Content-Type", "application/json");
  }
  headers.set("Accept", "application/json");

  try {
    const response = await fetch(`${API_BASE}${path}`, {
      ...requestOptions,
      headers,
      signal: controller.signal,
      body:
        requestOptions.body !== undefined && !(requestOptions.body instanceof FormData)
          ? JSON.stringify(requestOptions.body)
          : requestOptions.body,
    });
    if (response.status === 204) return null;
    const contentType = response.headers.get("content-type") ?? "";
    let payload = null;
    if (contentType.includes("json")) {
      payload = await response.json();
    } else {
      const text = await response.text();
      payload = text ? { detail: text } : null;
    }
    if (response.status === 401 && path !== "/session" && !_authRetried) {
      await waitForAuthentication();
      return api(path, { ...options, _authRetried: true });
    }
    if (!response.ok) throw extractApiError(payload, response.status);
    return payload;
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new ApiError("请求超时，请检查服务或存储连接。", 0);
    }
    if (error instanceof ApiError) throw error;
    throw new ApiError("无法连接 EasyBackup 服务。", 0, error);
  } finally {
    window.clearTimeout(timeout);
  }
}

function waitForAuthentication() {
  if (state.authPromise) return state.authPromise;
  state.authPromise = new Promise((resolve) => {
    state.authResolve = resolve;
  });
  $("#authFormStatus").textContent = "";
  openDialog("authDialog");
  window.setTimeout(() => $("#authToken").focus(), 0);
  return state.authPromise;
}

async function submitAuthentication(event) {
  event.preventDefault();
  const tokenInput = $("#authToken");
  const statusNode = $("#authFormStatus");
  const button = $("#authSubmitButton");
  const token = tokenInput.value;
  if (!token) {
    statusNode.textContent = "请输入 API Token。";
    tokenInput.focus();
    return;
  }
  setButtonBusy(button, true, "正在验证…");
  statusNode.textContent = "";
  try {
    await api("/session", {
      method: "POST",
      body: { token },
    });
    tokenInput.value = "";
    closeDialog("authDialog");
    const resolve = state.authResolve;
    state.authResolve = null;
    state.authPromise = null;
    if (resolve) resolve();
    showToast("身份验证成功", "安全会话已建立。");
  } catch (error) {
    statusNode.textContent = error.message;
    tokenInput.select();
  } finally {
    setButtonBusy(button, false);
  }
}

function showToast(title, message = "", tone = "success", duration = 4800) {
  const region = $("#toastRegion");
  const toast = element("div", `toast ${tone}`, undefined);
  toast.setAttribute("role", tone === "error" ? "alert" : "status");

  const symbol = element(
    "span",
    "toast-symbol",
    tone === "error" ? "!" : tone === "warning" ? "i" : "✓",
  );
  const body = element("div", "toast-body");
  append(body, element("strong", "", title), message ? element("p", "", message) : null);
  const closeButton = element("button", "toast-close", "×");
  closeButton.type = "button";
  closeButton.setAttribute("aria-label", "关闭通知");
  append(toast, symbol, body, closeButton);
  region.append(toast);

  const remove = () => {
    if (!toast.isConnected) return;
    toast.classList.add("is-leaving");
    window.setTimeout(() => toast.remove(), 210);
  };
  closeButton.addEventListener("click", remove);
  window.setTimeout(remove, duration);
}

function createEmptyState(container, title, message, actionLabel = "", action = "") {
  clear(container);
  const fragment = $("#emptyStateTemplate").content.cloneNode(true);
  $(".empty-state h3", fragment).textContent = title;
  $(".empty-state p", fragment).textContent = message;
  const button = $(".empty-state button", fragment);
  if (actionLabel && action) {
    button.textContent = actionLabel;
    button.dataset.action = action;
    button.classList.remove("is-hidden");
  }
  container.append(fragment);
}

function showLoading(container, rows = 2) {
  clear(container);
  for (let index = 0; index < rows; index += 1) {
    container.append(element("div", "loading-row"));
  }
}

function openDialog(dialogId) {
  const dialog = document.getElementById(dialogId);
  if (!dialog) return;
  if (typeof dialog.showModal === "function") {
    if (!dialog.open) dialog.showModal();
  } else {
    dialog.setAttribute("open", "");
  }
}

function closeDialog(dialogId) {
  const dialog = document.getElementById(dialogId);
  if (!dialog) return;
  if (typeof dialog.close === "function" && dialog.open) dialog.close();
  else dialog.removeAttribute("open");
  if (dialogId === "credentialDialog") {
    $("#credentialForm").reset();
    $("#credentialSecretKey").type = "password";
    $("#toggleSecretButton").textContent = "显示";
  }
}

function clearFormErrors(form) {
  $$(".field.has-error", form).forEach((field) => field.classList.remove("has-error"));
  $$(".field-error", form).forEach((node) => {
    node.textContent = "";
  });
}

function setFieldError(form, path, message) {
  if (!path) return false;
  const candidates = [
    path,
    path.replace(/^task\./, ""),
    path.replace(/^storage\.(?:s3|local)\./, "storage."),
    path.replace(/^storage\./, "storage."),
    path.split(".").slice(-1)[0],
  ];
  let target = null;
  for (const candidate of candidates) {
    target = $(`[data-error-for="${candidate}"]`, form);
    if (target) break;
  }
  if (!target) return false;
  target.textContent = message;
  const field = target.closest(".field");
  if (field) field.classList.add("has-error");
  return true;
}

function applyFormError(form, error, statusNode = null) {
  clearFormErrors(form);
  let applied = false;
  for (const issue of error.fieldErrors ?? []) {
    applied = setFieldError(form, issue.path, issue.message) || applied;
  }
  if (statusNode) statusNode.textContent = applied ? "请检查标记的字段。" : error.message;
  if (!applied) showToast("无法保存", error.message, "error");
}

function navigate(viewName, updateHash = true) {
  if (!VIEW_META[viewName]) viewName = "dashboard";
  state.view = viewName;
  $$(".view").forEach((view) => view.classList.toggle("is-active", view.dataset.viewPanel === viewName));
  $$(".nav-item").forEach((item) => item.classList.toggle("is-active", item.dataset.view === viewName));
  const meta = VIEW_META[viewName];
  $("#pageEyebrow").textContent = meta.eyebrow;
  $("#pageTitle").textContent = meta.title;
  $("#pageSubtitle").textContent = meta.subtitle;
  document.title = `${meta.title} · EasyBackup`;
  if (updateHash && window.location.hash !== `#/${viewName}`) {
    history.replaceState(null, "", `#/${viewName}`);
  }
  closeMobileMenu();
  $("#mainContent").focus({ preventScroll: true });

  if (viewName === "snapshots") {
    ensureSnapshotTaskSelected();
  } else if (viewName === "operations") {
    loadOperations({ silent: true });
  } else if (viewName === "system") {
    loadSystem({ silent: true });
  } else if (viewName === "credentials") {
    loadCredentials({ silent: true });
  }
}

function openMobileMenu() {
  $("#sidebar").classList.add("is-open");
  $("#mobileScrim").classList.add("is-open");
  $("#menuButton").setAttribute("aria-expanded", "true");
}

function closeMobileMenu() {
  $("#sidebar").classList.remove("is-open");
  $("#mobileScrim").classList.remove("is-open");
  $("#menuButton").setAttribute("aria-expanded", "false");
}

function setConnection(mode) {
  state.wsState = mode;
  const dot = $("#connectionDot");
  dot.classList.remove("online", "offline");
  if (mode === "online") {
    dot.classList.add("online");
    $("#connectionLabel").textContent = "实时连接正常";
    $("#connectionHint").textContent = "进度正在同步";
  } else if (mode === "offline") {
    dot.classList.add("offline");
    $("#connectionLabel").textContent = "实时连接中断";
    $("#connectionHint").textContent = "正在自动重连";
  } else {
    $("#connectionLabel").textContent = "正在连接";
    $("#connectionHint").textContent = "等待实时通道";
  }
}

function normalizeTools(system) {
  const raw = system?.tools ?? system?.external_tools ?? {};
  if (Array.isArray(raw)) {
    return raw.map((tool, index) => ({
      name: tool.name ?? tool.id ?? `工具 ${index + 1}`,
      available: tool.available ?? tool.ok ?? tool.found ?? Boolean(tool.path || tool.version),
      version: tool.version ?? tool.path ?? tool.message ?? "",
    }));
  }
  return Object.entries(raw).map(([name, value]) => {
    if (typeof value === "boolean") return { name, available: value, version: "" };
    if (typeof value === "string") {
      const missing = ["missing", "not found", "unavailable", "false"].includes(value.toLowerCase());
      return { name, available: !missing, version: missing ? "" : value };
    }
    return {
      name,
      available: value?.available ?? value?.ok ?? value?.found ?? Boolean(value?.path || value?.version),
      version: value?.version ?? value?.path ?? value?.message ?? value?.error ?? "",
    };
  });
}

function toolDisplayName(name) {
  const normalized = String(name).toLowerCase();
  if (normalized.includes("zstd")) return "Zstandard";
  if (normalized === "7z" || normalized.includes("7zip") || normalized.includes("seven")) return "7-Zip";
  if (normalized.includes("tar")) return "Tar";
  if (normalized.includes("gzip")) return "Gzip";
  return name;
}

function systemIsHealthy(system) {
  if (!system) return false;
  if (typeof system.healthy === "boolean") return system.healthy;
  if (typeof system.ok === "boolean") return system.ok;
  const statusText = String(system.status ?? system.health ?? "").toLowerCase();
  if (["failed", "error", "unhealthy", "offline"].includes(statusText)) return false;
  if (["ok", "healthy", "running", "ready"].includes(statusText)) return true;
  const db = system.database;
  if (typeof db === "boolean" && !db) return false;
  if (typeof db === "object" && (db?.ok === false || db?.healthy === false)) return false;
  return true;
}

function renderTools(container, tools, systemView = false) {
  clear(container);
  if (!tools.length) {
    createEmptyState(container, "尚无工具信息", "服务未返回外部工具探测结果。");
    return;
  }

  for (const tool of tools) {
    if (systemView) {
      const card = element("article", "system-tool");
      const head = element("div", "system-tool-head");
      append(
        head,
        element("h3", "", toolDisplayName(tool.name)),
        statusBadge(tool.available ? "completed" : "failed", tool.available ? "可用" : "缺失"),
      );
      append(card, head, element("p", "", tool.version || (tool.available ? "已探测到可执行文件" : "请安装并加入 PATH")));
      container.append(card);
    } else {
      const row = element("div", "tool-row");
      const mark = element("span", `tool-state${tool.available ? "" : " missing"}`, tool.available ? "✓" : "!");
      const copy = element("span");
      append(
        copy,
        element("strong", "", toolDisplayName(tool.name)),
        element("small", "", tool.version || (tool.available ? "已就绪" : "未检测到")),
      );
      append(row, mark, copy, statusBadge(tool.available ? "completed" : "failed", tool.available ? "就绪" : "缺失"));
      container.append(row);
    }
  }
}

function renderSystem() {
  const system = state.system;
  const healthy = systemIsHealthy(system);
  const badge = $("#systemOverallBadge");
  badge.className = `status-badge ${healthy ? "completed" : "failed"}`;
  badge.textContent = healthy ? "服务就绪" : "需要处理";

  const details = $("#systemDetails");
  clear(details);
  const valueOf = (value) => {
    if (value === undefined || value === null || value === "") return "—";
    if (typeof value === "boolean") return value ? "正常" : "异常";
    if (typeof value === "string" || typeof value === "number") return String(value);
    return value.status ?? value.backend ?? value.path ?? value.message ?? (value.ok === true ? "正常" : value.ok === false ? "异常" : "已配置");
  };
  const rows = [
    ["服务版本", system?.version ?? system?.app_version],
    ["服务状态", system?.status ?? system?.health ?? (system ? "running" : null)],
    ["数据库", system?.database ?? system?.db],
    ["调度器", system?.scheduler],
    ["凭据后端", system?.credential_backend ?? system?.keyring],
    ["服务时区", system?.timezone],
    ["数据目录", system?.data_dir],
    ["活动操作", system?.active_operations ?? state.operations.filter((op) => ACTIVE_STATUSES.has(op.status)).length],
  ];
  for (const [label, rawValue] of rows) {
    const row = element("div", "definition-row");
    append(row, element("span", "", label), element("strong", "", valueOf(rawValue)));
    details.append(row);
  }

  const tools = normalizeTools(system);
  renderTools($("#systemTools"), tools, true);
  renderTools($("#dashboardTools"), tools, false);
  $("#metricHealth").textContent = healthy ? "良好" : "需处理";
  $("#metricHealthHint").textContent = healthy ? "核心服务已就绪" : "请查看系统状态";
  renderSidebarOverview();
}

async function loadSystem({ silent = false } = {}) {
  try {
    state.system = await api("/system/status");
    renderSystem();
    return state.system;
  } catch (error) {
    state.system = null;
    renderSystem();
    if (!silent) showToast("系统状态读取失败", error.message, "error");
    return null;
  }
}

function lastOperationForTask(taskId) {
  return [...state.operations]
    .filter((operation) => operation.task_id === taskId)
    .sort((a, b) => new Date(b.created_at ?? 0) - new Date(a.created_at ?? 0))[0];
}

function taskTargetLabel(task) {
  const storage = task.storage ?? {};
  if (storage.kind === "s3") {
    const prefix = storage.prefix ? `/${storage.prefix}` : "";
    return `s3://${storage.bucket ?? "—"}${prefix}`;
  }
  return storage.path ?? "未配置目标";
}

function taskScheduleLabel(task) {
  if (!task.schedule) return "仅手动运行";
  return `Cron · ${task.schedule}`;
}

function schedulePresetFor(schedule) {
  const normalized = String(schedule ?? "").trim();
  const match = Object.entries(SCHEDULE_PRESETS).find(([, value]) => value === normalized);
  return match?.[0] ?? "custom";
}

function applySchedulePreset(preset, { preserveValue = false } = {}) {
  const schedule = $("#taskSchedule");
  const custom = preset === "custom";
  if (!custom && !preserveValue) schedule.value = SCHEDULE_PRESETS[preset] ?? "";
  schedule.readOnly = !custom;
  $("#taskScheduleField").classList.toggle("is-custom", custom);
  schedule.placeholder = custom
    ? "例如：0 2 * * *"
    : preset === "manual"
      ? "当前仅手动运行"
      : "已由运行频率自动生成";
}

function updateFullEveryPresets() {
  const value = String(numberOr($("#taskFullEvery").value, 6));
  $$("[data-full-every]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.fullEvery === value);
  });
}

function setStorageProbeStatus(message, mode = "") {
  const status = $("#storageTestStatus");
  const bar = status.closest(".storage-test-bar");
  status.textContent = message;
  status.className = mode ? `is-${mode}` : "";
  state.storageProbeMode = mode;
  if (bar) {
    bar.classList.remove("is-testing", "is-success", "is-error", "is-stale");
    if (mode) bar.classList.add(`is-${mode}`);
  }
}

function clearStorageDiagnostic() {
  const panel = $("#storageDiagnosticPanel");
  if (!panel) return;
  panel.classList.add("is-hidden");
  $("#storageDiagnosticTitle").textContent = "存储配置不可用";
  $("#storageDiagnosticSummary").textContent = "";
  clear($("#storageDiagnosticFacts"));
  clear($("#storageDiagnosticSuggestions"));
  $("#storageDiagnosticActions").classList.remove("is-hidden");
}

function normalizeDiagnosticSuggestions(value) {
  const entries = Array.isArray(value) ? value : value ? [value] : [];
  return entries
    .map((entry) => {
      if (typeof entry === "string") return entry.trim();
      if (entry && typeof entry === "object") {
        return String(entry.message ?? entry.text ?? entry.suggestion ?? "").trim();
      }
      return "";
    })
    .filter(Boolean);
}

function fallbackStorageDiagnostic(error, storage) {
  const payload = error?.payload && typeof error.payload === "object" ? error.payload : {};
  const diagnosticKind = String(payload?.details?.diagnostic?.kind ?? "").trim();
  const problemCode = String(payload?.code ?? payload?.error?.problem_code ?? "").trim();
  const providerCode = String(
    payload?.details?.provider_code ??
      payload?.error?.code ??
      payload?.provider_code ??
      "",
  ).trim();
  const signal = `${diagnosticKind} ${problemCode} ${providerCode} ${error?.message ?? ""}`
    .toLowerCase()
    .replace(/[\s_-]+/g, "");
  let title = "无法验证存储配置";
  let summary = error?.message || "存储服务未返回可识别的诊断信息。";
  let suggestions = [
    "核对存储类型、Endpoint、Bucket、Region 与凭据配置后重新检测。",
    "展开下方“常见配置错误”，按网络、权限和签名顺序排查。",
    "如仍失败，请保留服务商错误码和请求 ID，以便进一步定位。",
  ];

  if (signal.includes("publicendpointforbidden")) {
    title = "当前环境禁止使用公网 Endpoint";
    summary = "对象存储拒绝了公网服务地址，通常与 Bucket、账号或运行环境的网络访问策略有关。";
    suggestions = [
      "阿里云 OSS 可为 Bucket 绑定自定义域名（CNAME）后改用该地址。",
      "若 EasyBackup 运行在 Bucket 同地域的阿里云网络中，可改用内网 Endpoint。",
      "检查 Bucket、账号与运行环境是否还有额外网络访问限制。",
    ];
  } else if (
    signal.includes("invalidendpoint") ||
    signal.includes("invalidurl") ||
    signal.includes("endpointurlformat")
  ) {
    title = "Endpoint URL 格式不完整";
    summary = "Endpoint 必须包含有效主机名；EasyBackup 会为裸域名自动补全 HTTPS。";
    suggestions = [
      "建议使用 https:// 开头的完整服务地址。",
      "阿里云上海地域可填写 https://s3.oss-cn-shanghai.aliyuncs.com。",
      "Endpoint 中不要包含 Bucket 名称、s3:// 前缀、查询参数或账号密码。",
    ];
  } else if (
    signal.includes("endpointconnection") ||
    signal.includes("endpointunreachable") ||
    signal.includes("timeout") ||
    signal.includes("timedout") ||
    signal.includes("dns") ||
    signal.includes("nameresolution") ||
    signal.includes("getaddrinfo") ||
    signal.includes("ssl") ||
    signal.includes("tls") ||
    signal.includes("certificate")
  ) {
    title = "无法建立 DNS / TLS 连接";
    summary = "主机无法解析或安全连接到 Endpoint，请先检查网络路径、代理和证书。";
    suggestions = [
      "确认运行 EasyBackup 的主机能够解析 Endpoint 域名并访问 443 端口。",
      "检查防火墙、系统代理、企业 TLS 检查和自签名证书设置。",
      "若使用内网 Endpoint，请确认当前主机位于对应 VPC 或已配置专线 / VPN。",
    ];
  } else if (
    diagnosticKind === "bucket" ||
    signal.includes("nosuchbucket") ||
    signal.includes("regionendpoint") ||
    signal.includes("bucketnotexist") ||
    signal.includes("invalidbucket") ||
    signal.includes("permanentredirect") ||
    signal.includes("regionmismatch") ||
    signal.includes("authorizationheadermalformed")
  ) {
    title = "Bucket 或 Region 不匹配";
    summary = "服务地址可以访问，但目标 Bucket 不存在、名称有误，或 Region 与 Bucket 所在地域不一致。";
    suggestions = [
      "Bucket 只填写名称，不要带 s3://、路径或 Endpoint 域名。",
      "在云控制台确认 Bucket 所在地域，并填写对应 Region。",
      "确保 Endpoint 与 Region 指向同一地域，例如上海对应 cn-shanghai。",
    ];
  } else if (
    signal.includes("signaturedoesnotmatch") ||
    signal.includes("signature") ||
    signal.includes("clock") ||
    signal.includes("invalidsignature") ||
    signal.includes("requesttimetoolskewed") ||
    signal.includes("requestexpired")
  ) {
    title = "请求签名或系统时间无效";
    summary = "对象存储无法验证当前请求签名，常见原因是密钥、Region、Endpoint 或系统时间不正确。";
    suggestions = [
      "同步运行 EasyBackup 主机的系统时间和时区后重试。",
      "重新核对 AK/SK，避免复制到前后空格、混用旧密钥或遗漏临时 Token。",
      "确认 Region 和 Endpoint 与服务商的 S3 兼容签名要求一致。",
    ];
  } else if (
    signal.includes("invalidaccesskey") ||
    signal.includes("credentialprofile") ||
    signal.includes("credential") ||
    signal.includes("permission") ||
    signal.includes("accessdenied") ||
    signal.includes("unauthorized") ||
    signal.includes("forbidden")
  ) {
    title = "凭据不存在或权限不足";
    summary = "当前凭据无法完成探针所需的写入、读取元数据、读取和删除操作。";
    suggestions = [
      "确认任务引用的凭据配置名称正确，且 AK/SK 仍然有效。",
      "授予该凭据对目标 Bucket 和对象前缀的写入、读取与删除权限。",
      "若使用临时凭据，请同时填写有效的 Session Token 并检查过期时间。",
    ];
  } else if (
    signal.includes("unsupportedoperation") ||
    signal.includes("addressingstyle")
  ) {
    title = "当前 S3 兼容模式不受支持";
    summary = "服务商不支持探针使用的对象操作，或 Bucket 寻址方式与当前 Endpoint 不兼容。";
    suggestions = [
      "确认填写的是服务商提供的 S3 兼容 Endpoint，而不是原生 API 或控制台地址。",
      "检查服务商是否要求 path-style 或 virtual-hosted-style Bucket 寻址。",
      "确认兼容接口支持写入、读取元数据、读取和删除对象。",
    ];
  } else if (signal.includes("throttled") || signal.includes("slowdown")) {
    title = "对象存储请求受到限流";
    summary = "服务商暂时限制了探针请求频率，并不一定表示配置错误。";
    suggestions = [
      "稍候片刻再重新检测，避免短时间连续提交连接测试。",
      "检查账号或 Bucket 的请求配额与流控告警。",
      "若持续发生，请携带请求 ID 联系服务商确认限流原因。",
    ];
  }

  return {
    title,
    summary,
    suggestions,
    kind: diagnosticKind || null,
    problem_code: problemCode || null,
    provider_code: providerCode || null,
    http_status: payload?.status ?? error?.status ?? null,
    request_id:
      payload?.details?.request_id ??
      payload?.error?.request_id ??
      payload?.request_id ??
      null,
    endpoint: storage?.kind === "s3" ? storage.endpoint_url : null,
    bucket: storage?.kind === "s3" ? storage.bucket : null,
    region: storage?.kind === "s3" ? storage.region : null,
  };
}

function storageDiagnosticFromError(error, storage) {
  const payload = error?.payload && typeof error.payload === "object" ? error.payload : {};
  const supplied = payload?.details?.diagnostic;
  const fallback = fallbackStorageDiagnostic(error, storage);
  if (!supplied || typeof supplied !== "object" || Array.isArray(supplied)) {
    return fallback;
  }

  const suggestions = normalizeDiagnosticSuggestions(supplied.suggestions);
  return {
    ...fallback,
    kind: supplied.kind ?? fallback.kind,
    title: supplied.title || payload.title || fallback.title,
    summary: supplied.summary || payload.detail || fallback.summary,
    suggestions: suggestions.length ? suggestions : fallback.suggestions,
    problem_code: supplied.problem_code ?? payload.code ?? fallback.problem_code,
    provider_code: supplied.provider_code ?? fallback.provider_code,
    http_status: supplied.http_status ?? payload.status ?? fallback.http_status,
    request_id: supplied.request_id ?? fallback.request_id,
    endpoint: supplied.endpoint ?? fallback.endpoint,
    bucket: supplied.bucket ?? fallback.bucket,
    region: supplied.region ?? fallback.region,
    operation: supplied.operation ?? fallback.operation,
  };
}

function renderStorageDiagnostic(diagnostic) {
  const panel = $("#storageDiagnosticPanel");
  const facts = $("#storageDiagnosticFacts");
  const suggestions = $("#storageDiagnosticSuggestions");
  clear(facts);
  clear(suggestions);
  $("#storageDiagnosticTitle").textContent =
    diagnostic?.title || "存储配置不可用";
  $("#storageDiagnosticSummary").textContent =
    diagnostic?.summary || "请检查存储配置后重试。";

  for (const [key, label] of STORAGE_DIAGNOSTIC_FACTS) {
    const value = diagnostic?.[key];
    if (value === undefined || value === null || value === "") continue;
    const row = element("div", "storage-diagnostic-fact");
    append(row, element("dt", "", label), element("dd", "", value));
    facts.append(row);
  }

  const items = normalizeDiagnosticSuggestions(diagnostic?.suggestions);
  for (const item of items) suggestions.append(element("li", "", item));
  $("#storageDiagnosticActions").classList.toggle("is-hidden", !items.length);
  panel.classList.remove("is-hidden");
}

function resetStorageProbe() {
  state.storageProbeRevision += 1;
  clearStorageDiagnostic();
  setStorageProbeStatus(STORAGE_PROBE_IDLE_MESSAGE);
}

function invalidateStorageProbe() {
  const previousMode = state.storageProbeMode;
  state.storageProbeRevision += 1;
  clearStorageDiagnostic();
  if (state.storageProbePending) {
    setStorageProbeStatus(
      "配置已变化；当前检测完成后将标记为过期，请重新检测。",
      "stale",
    );
  } else if (["success", "error", "stale"].includes(previousMode)) {
    setStorageProbeStatus("配置已修改，上次检测结果已失效，请重新检测。", "stale");
  }
}

function renderSidebarOverview() {
  const enabledTasks = state.tasks.filter((task) => task.enabled);
  const targetKinds = new Set(
    enabledTasks.map((task) => (task.storage?.kind === "s3" ? "S3 / OSS" : "本地")),
  );
  const scheduler = state.system?.scheduler;
  const schedulerRunning =
    typeof scheduler === "object" ? scheduler?.running : Boolean(scheduler);
  const credentialBackend = state.system?.credential_backend ?? state.system?.keyring;
  const credentialLabel =
    credentialBackend?.backend ??
    credentialBackend?.status ??
    (credentialBackend?.keyring_available
      ? credentialBackend.keyring_backend ?? "系统 Keyring"
      : credentialBackend?.encrypted_file_available
        ? "本地加密存储"
        : null) ??
    (typeof credentialBackend === "string" ? credentialBackend : null);
  const nextRuns = enabledTasks
    .map((task) => task.next_run_at)
    .filter(Boolean)
    .sort((a, b) => new Date(a) - new Date(b));

  $("#sidebarProtectedTasks").textContent = enabledTasks.length;
  $("#sidebarStorageKinds").textContent = targetKinds.size ? [...targetKinds].join(" + ") : "尚未配置";
  $("#sidebarSnapshotCount").textContent =
    state.system?.snapshot_count ?? state.snapshots.length ?? 0;
  $("#sidebarCredentialState").textContent = credentialLabel
    ? `${credentialLabel}${state.credentials.length ? ` · ${state.credentials.length} 组` : ""}`
    : state.credentials.length
      ? `${state.credentials.length} 组已托管`
      : "检测中";
  $("#sidebarNextRun").textContent = nextRuns.length ? formatRelative(nextRuns[0]) : "仅手动运行";
  $("#sidebarNextRun").title = nextRuns.length ? formatDate(nextRuns[0]) : "";
  const daemon = $("#sidebarDaemonState");
  daemon.textContent = state.system ? (schedulerRunning ? "● Daemon 运行中" : "● Daemon 未运行") : "检测中";
  daemon.classList.toggle("is-offline", Boolean(state.system) && !schedulerRunning);

  const ring = $(".storage-ring");
  if (ring) {
    const protectedRatio = state.tasks.length ? enabledTasks.length / state.tasks.length : 0;
    ring.style.setProperty("--storage-progress", `${Math.round(protectedRatio * 100)}%`);
  }
}

function renderTaskCard(task) {
  const card = element("article", `task-card${task.enabled ? "" : " is-disabled"}`);
  const head = element("div", "task-card-head");
  const title = element("div", "task-card-title");
  const storageKind = task.storage?.kind === "s3" ? "s3" : "local";
  const kindMark = element("span", `task-kind-mark ${storageKind}`, storageKind === "s3" ? "☁" : "▰");
  const titleCopy = element("div");
  append(titleCopy, element("h2", "", task.name), element("p", "", task.source_path));
  append(title, kindMark, titleCopy);

  const menu = element("div", "task-card-menu");
  const editButton = element("button", "icon-button", "✎");
  editButton.type = "button";
  editButton.dataset.action = "edit-task";
  editButton.dataset.taskId = task.id;
  editButton.setAttribute("aria-label", `编辑任务 ${task.name}`);
  editButton.title = "编辑";
  const deleteButton = element("button", "icon-button", "×");
  deleteButton.type = "button";
  deleteButton.dataset.action = "delete-task";
  deleteButton.dataset.taskId = task.id;
  deleteButton.setAttribute("aria-label", `删除任务 ${task.name}`);
  deleteButton.title = "删除";
  append(menu, editButton, deleteButton);
  append(head, title, menu);

  const meta = element("div", "task-meta-grid");
  const target = element("div", "task-meta");
  append(target, element("span", "", "存储目标"), element("strong", "", taskTargetLabel(task)));
  const schedule = element("div", "task-meta");
  append(schedule, element("span", "", "运行计划"), element("strong", "", taskScheduleLabel(task)));
  const strategy = element("div", "task-meta");
  append(
    strategy,
    element("span", "", "备份策略"),
    element("strong", "", `${task.full_every ?? 6} 次增量后全量 · ${String(task.compression ?? "auto").toUpperCase()}`),
  );
  const retention = element("div", "task-meta");
  append(
    retention,
    element("span", "", "保留策略"),
    element("strong", "", `${task.retention_chains ?? 3} 条链 / ${task.retention_days ?? 30} 天`),
  );
  const delta = element("div", "task-meta");
  append(
    delta,
    element("span", "", "大文件差分"),
    element(
      "strong",
      "",
      task.delta_enabled === false
        ? "差分关闭"
        : `≥ ${task.delta_threshold_mb ?? 100} MB / 最高 ${Math.round(numberOr(task.delta_max_ratio, 0.9) * 100)}%`,
    ),
  );
  append(meta, target, schedule, strategy, retention, delta);

  const foot = element("div", "task-card-foot");
  const left = element("div");
  const last = lastOperationForTask(task.id);
  append(
    left,
    statusBadge(task.enabled ? "completed" : "disabled", task.enabled ? "已启用" : "已停用"),
  );
  if (last) {
    const recent = element("span", "selection-count", `最近运行 ${formatRelative(last.completed_at ?? last.created_at)}`);
    recent.style.marginLeft = "8px";
    left.append(recent);
  }
  const actions = element("div", "inline-actions");
  const toggleButton = element("button", "button button-secondary", task.enabled ? "停用" : "启用");
  toggleButton.type = "button";
  toggleButton.dataset.action = "toggle-task";
  toggleButton.dataset.taskId = task.id;
  const snapshotButton = element("button", "button button-secondary", "快照");
  snapshotButton.type = "button";
  snapshotButton.dataset.action = "view-snapshots";
  snapshotButton.dataset.taskId = task.id;
  const runButton = element("button", "button button-primary", "立即运行");
  runButton.type = "button";
  runButton.dataset.action = "run-task";
  runButton.dataset.taskId = task.id;
  const active = state.operations.some(
    (operation) => operation.task_id === task.id && ACTIVE_STATUSES.has(operation.status),
  );
  if (active) {
    runButton.disabled = true;
    runButton.textContent = "正在运行";
  }
  append(actions, toggleButton, snapshotButton, runButton);
  append(foot, left, actions);
  append(card, head, meta, foot);
  return card;
}

function filteredTasks() {
  const query = state.taskFilter.query.trim().toLocaleLowerCase("zh-CN");
  return state.tasks.filter((task) => {
    if (state.taskFilter.status === "enabled" && !task.enabled) return false;
    if (state.taskFilter.status === "disabled" && task.enabled) return false;
    if (!query) return true;
    const haystack = [
      task.name,
      task.source_path,
      taskTargetLabel(task),
      task.schedule,
    ]
      .join(" ")
      .toLocaleLowerCase("zh-CN");
    return haystack.includes(query);
  });
}

function renderTasks() {
  const container = $("#taskList");
  clear(container);
  const tasks = filteredTasks();
  if (!state.tasks.length) {
    createEmptyState(container, "还没有备份任务", "创建第一个任务，让重要数据自动拥有可恢复的历史版本。", "新建任务", "new-task");
  } else if (!tasks.length) {
    createEmptyState(container, "没有匹配的任务", "尝试修改搜索内容或状态筛选。");
  } else {
    tasks.forEach((task) => container.append(renderTaskCard(task)));
  }

  $("#navTaskCount").textContent = state.tasks.length;
  $("#metricTasks").textContent = state.tasks.length;
  const enabled = state.tasks.filter((task) => task.enabled).length;
  $("#metricTasksHint").textContent = `${enabled} 个已启用`;
  renderDashboardTasks();
  populateTaskOptions();
  renderSidebarOverview();
}

function renderDashboardTasks() {
  const container = $("#dashboardTasks");
  clear(container);
  if (!state.tasks.length) {
    createEmptyState(container, "尚未设置备份计划", "创建任务后，运行计划会显示在这里。", "创建任务", "new-task");
    return;
  }
  const sorted = [...state.tasks].sort((a, b) => Number(b.enabled) - Number(a.enabled)).slice(0, 6);
  for (const task of sorted) {
    const card = element("article", "compact-task");
    const head = element("div", "compact-task-head");
    append(head, element("h3", "", task.name), statusBadge(task.enabled ? "completed" : "disabled", task.enabled ? "启用" : "停用"));
    const footer = element("footer");
    const last = lastOperationForTask(task.id);
    append(
      footer,
      element("span", "", taskScheduleLabel(task)),
      element("span", "", last ? formatRelative(last.completed_at ?? last.created_at) : "尚未运行"),
    );
    append(card, head, element("p", "", `${task.source_path} → ${taskTargetLabel(task)}`), footer);
    container.append(card);
  }
}

function populateTaskOptions() {
  const select = $("#snapshotTaskFilter");
  const current = state.snapshotTaskId || select.value;
  clear(select);
  const placeholder = element("option", "", "请选择任务");
  placeholder.value = "";
  select.append(placeholder);
  for (const task of state.tasks) {
    const option = element("option", "", task.name);
    option.value = task.id;
    select.append(option);
  }
  if (state.tasks.some((task) => task.id === current)) {
    select.value = current;
  } else {
    state.snapshotTaskId = "";
    select.value = "";
  }

  const datalist = $("#credentialProfileOptions");
  clear(datalist);
  for (const credential of state.credentials) {
    const option = element("option");
    option.value = credential.profile;
    datalist.append(option);
  }
}

async function loadTasks({ silent = false } = {}) {
  try {
    state.tasks = unwrapList(await api("/tasks"));
    renderTasks();
    return state.tasks;
  } catch (error) {
    state.tasks = [];
    renderTasks();
    if (!silent) showToast("任务读取失败", error.message, "error");
    return [];
  }
}

function resetTaskForm() {
  const form = $("#taskForm");
  form.reset();
  clearFormErrors(form);
  $("#taskId").value = "";
  $("#taskDialogTitle").textContent = "新建备份任务";
  $("#taskFormStatus").textContent = "";
  resetStorageProbe();
  $("#taskEnabled").checked = true;
  $("#taskCompression").value = "auto";
  $("#taskCompressionLevel").value = "3";
  $("#taskShardSize").value = "256";
  $("#taskFullEvery").value = "6";
  $("#taskSchedulePreset").value = "manual";
  $("#taskRetentionChains").value = "3";
  $("#taskRetentionDays").value = "30";
  $("#taskDeltaEnabled").checked = true;
  $("#taskDeltaThreshold").value = "100";
  $("#taskDeltaMaxRatio").value = "90";
  $("#taskExcludes").value = DEFAULT_EXCLUDES.join("\n");
  $(`input[name="storage_kind"][value="local"]`).checked = true;
  $("#s3Prefix").value = "easybackup";
  $("#s3CredentialProfile").value = "default";
  $("#s3ChunkSize").value = "16";
  toggleStorageFields("local");
  toggleDeltaFields(true);
  applySchedulePreset("manual");
  updateFullEveryPresets();
}

function toggleStorageFields(kind) {
  const isS3 = kind === "s3";
  $("#localStorageFields").classList.toggle("is-hidden", isS3);
  $("#s3StorageFields").classList.toggle("is-hidden", !isS3);
}

function toggleDeltaFields(enabled) {
  $("#deltaSettings").classList.toggle("is-disabled", !enabled);
  $("#taskDeltaThreshold").disabled = !enabled;
  $("#taskDeltaMaxRatio").disabled = !enabled;
}

function openTaskForm(taskId = "") {
  resetTaskForm();
  if (taskId) {
    const task = taskById(taskId);
    if (!task) return;
    $("#taskId").value = task.id;
    $("#taskDialogTitle").textContent = "编辑备份任务";
    $("#taskName").value = task.name ?? "";
    $("#taskSourcePath").value = task.source_path ?? "";
    $("#taskSchedule").value = task.schedule ?? "";
    const schedulePreset = schedulePresetFor(task.schedule);
    $("#taskSchedulePreset").value = schedulePreset;
    applySchedulePreset(schedulePreset, { preserveValue: true });
    $("#taskEnabled").checked = task.enabled !== false;
    $("#taskCompression").value = task.compression ?? "auto";
    $("#taskCompressionLevel").value = task.compression_level ?? 3;
    $("#taskShardSize").value = task.shard_size_mb ?? 256;
    $("#taskFullEvery").value = task.full_every ?? 6;
    updateFullEveryPresets();
    $("#taskRetentionChains").value = task.retention_chains ?? 3;
    $("#taskRetentionDays").value = task.retention_days ?? 30;
    $("#taskFollowSymlinks").checked = Boolean(task.follow_symlinks);
    $("#taskDeltaEnabled").checked = task.delta_enabled !== false;
    $("#taskDeltaThreshold").value = task.delta_threshold_mb ?? 100;
    $("#taskDeltaMaxRatio").value = Math.round(
      numberOr(task.delta_max_ratio, 0.9) * 100,
    );
    toggleDeltaFields($("#taskDeltaEnabled").checked);
    $("#taskExcludes").value = Array.isArray(task.excludes) ? task.excludes.join("\n") : "";

    const kind = task.storage?.kind === "s3" ? "s3" : "local";
    $(`input[name="storage_kind"][value="${kind}"]`).checked = true;
    toggleStorageFields(kind);
    if (kind === "local") {
      $("#localStoragePath").value = task.storage?.path ?? "";
    } else {
      $("#s3Bucket").value = task.storage?.bucket ?? "";
      $("#s3Prefix").value = task.storage?.prefix ?? "easybackup";
      $("#s3Region").value = task.storage?.region ?? "";
      $("#s3Endpoint").value = task.storage?.endpoint_url ?? "";
      $("#s3CredentialProfile").value = task.storage?.credential_profile ?? "default";
      $("#s3StorageClass").value = task.storage?.storage_class ?? "";
      $("#s3ChunkSize").value = task.storage?.multipart_chunk_mb ?? 16;
    }
  }
  openDialog("taskDialog");
  window.setTimeout(() => $("#taskName").focus(), 20);
}

function collectTaskPayload() {
  const storage = collectStoragePayload();
  return {
    name: $("#taskName").value.trim(),
    source_path: $("#taskSourcePath").value.trim(),
    storage,
    schedule: $("#taskSchedule").value.trim() || null,
    enabled: $("#taskEnabled").checked,
    excludes: $("#taskExcludes")
      .value.split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean),
    compression: $("#taskCompression").value,
    compression_level: numberOr($("#taskCompressionLevel").value, 3),
    shard_size_mb: numberOr($("#taskShardSize").value, 256),
    full_every: numberOr($("#taskFullEvery").value, 6),
    retention_chains: numberOr($("#taskRetentionChains").value, 3),
    retention_days: numberOr($("#taskRetentionDays").value, 30),
    follow_symlinks: $("#taskFollowSymlinks").checked,
    delta_enabled: $("#taskDeltaEnabled").checked,
    delta_threshold_mb: numberOr($("#taskDeltaThreshold").value, 100),
    delta_max_ratio: numberOr($("#taskDeltaMaxRatio").value, 90) / 100,
  };
}

function setEndpointFormatError(message = "") {
  const target = $('[data-error-for="storage.endpoint_url"]', $("#taskForm"));
  if (!target) return;
  target.textContent = message;
  target.closest(".field")?.classList.toggle("has-error", Boolean(message));
}

function normalizeS3EndpointInput({ showError = false } = {}) {
  const input = $("#s3Endpoint");
  const raw = input.value.trim();
  if (!raw) {
    if (showError) setEndpointFormatError();
    return { value: null, error: "" };
  }

  const candidate = /^[a-z][a-z\d+.-]*:\/\//i.test(raw)
    ? raw
    : `https://${raw}`;
  let parsed;
  try {
    parsed = new URL(candidate);
  } catch {
    const error = "Endpoint URL 无法解析，请填写完整的服务地址。";
    if (showError) setEndpointFormatError(error);
    return { value: raw, error };
  }

  if (!["http:", "https:"].includes(parsed.protocol) || !parsed.hostname) {
    const error = "Endpoint URL 仅支持 http:// 或 https://。";
    if (showError) setEndpointFormatError(error);
    return { value: raw, error };
  }

  const hostname = parsed.hostname.toLowerCase();
  if (/^oss-[a-z0-9-]+\.aliyuncs\.com$/i.test(hostname)) {
    parsed.hostname = `s3.${hostname}`;
  }

  let normalized = parsed.toString();
  if (
    parsed.pathname === "/" &&
    !parsed.search &&
    !parsed.hash &&
    normalized.endsWith("/")
  ) {
    normalized = normalized.slice(0, -1);
  }
  input.value = normalized;
  if (showError) setEndpointFormatError();
  return { value: normalized, error: "" };
}

function collectStoragePayload() {
  const kind = $(`input[name="storage_kind"]:checked`)?.value ?? "local";
  if (kind === "s3") {
    const endpoint = normalizeS3EndpointInput();
    return {
      kind: "s3",
      bucket: $("#s3Bucket").value.trim(),
      prefix: $("#s3Prefix").value.trim() || "easybackup",
      region: $("#s3Region").value.trim() || null,
      endpoint_url: endpoint.value,
      credential_profile: $("#s3CredentialProfile").value.trim() || "default",
      storage_class: $("#s3StorageClass").value.trim() || null,
      multipart_chunk_mb: numberOr($("#s3ChunkSize").value, 16),
    };
  }
  return {
    kind: "local",
    path: $("#localStoragePath").value.trim(),
  };
}

async function testStorageConfiguration() {
  const form = $("#taskForm");
  const storage = collectStoragePayload();
  clearFormErrors(form);
  clearStorageDiagnostic();
  if (storage.kind === "local" && !storage.path) {
    setFieldError(form, "storage.path", "请先输入备份存放目录。");
    setStorageProbeStatus("请先补充本地存储路径。", "error");
    return;
  }
  if (storage.kind === "s3" && !storage.bucket) {
    setFieldError(form, "storage.bucket", "请先输入 Bucket 名称。");
    setStorageProbeStatus("请先补充 S3 / OSS Bucket。", "error");
    return;
  }
  const endpointValidation =
    storage.kind === "s3"
      ? normalizeS3EndpointInput({ showError: true })
      : { value: null, error: "" };
  if (endpointValidation.error) {
    const message = endpointValidation.error;
    setFieldError(form, "storage.endpoint_url", message);
    setStorageProbeStatus(message, "error");
    renderStorageDiagnostic(
      fallbackStorageDiagnostic(
        new ApiError(message, 0, { code: "INVALID_ENDPOINT_URL" }),
        storage,
      ),
    );
    return;
  }

  const button = $("#testStorageButton");
  const requestRevision = state.storageProbeRevision;
  state.storageProbePending = true;
  setButtonBusy(button, true, "检测中…");
  setStorageProbeStatus("正在执行写入、读取与删除探针…", "testing");
  try {
    const result = await api("/storage/test", {
      method: "POST",
      body: { storage },
    });
    if (requestRevision !== state.storageProbeRevision) {
      clearStorageDiagnostic();
      setStorageProbeStatus("检测结果已过期：配置在检测期间发生变化，请重新检测。", "stale");
      return;
    }
    clearStorageDiagnostic();
    setStorageProbeStatus(`✓ ${result.message}（${result.latency_ms} ms）`, "success");
    showToast("存储配置可用", `${result.target} · ${result.latency_ms} ms`);
  } catch (error) {
    if (requestRevision !== state.storageProbeRevision) {
      clearStorageDiagnostic();
      setStorageProbeStatus("检测结果已过期：配置在检测期间发生变化，请重新检测。", "stale");
      return;
    }
    for (const issue of error.fieldErrors ?? []) {
      setFieldError(form, issue.path, issue.message);
    }
    renderStorageDiagnostic(storageDiagnosticFromError(error, storage));
    setStorageProbeStatus(`检测失败：${error.message}`, "error");
    showToast("存储配置不可用", error.message, "error");
  } finally {
    state.storageProbePending = false;
    setButtonBusy(button, false);
  }
}

function validateTaskPayload(payload) {
  let valid = true;
  const form = $("#taskForm");
  clearFormErrors(form);
  if (!payload.name) {
    setFieldError(form, "name", "请输入任务名称。");
    valid = false;
  }
  if (!payload.source_path) {
    setFieldError(form, "source_path", "请输入源目录。");
    valid = false;
  }
  if (payload.storage.kind === "local" && !payload.storage.path) {
    setFieldError(form, "storage.path", "请输入备份存放目录。");
    valid = false;
  }
  if (payload.storage.kind === "s3" && !payload.storage.bucket) {
    setFieldError(form, "storage.bucket", "请输入 Bucket 名称。");
    valid = false;
  }
  if (payload.delta_enabled && payload.delta_threshold_mb < 1) {
    setFieldError(form, "delta_threshold_mb", "阈值至少为 1 MB。");
    valid = false;
  }
  if (
    payload.delta_enabled &&
    (payload.delta_max_ratio <= 0 || payload.delta_max_ratio > 1)
  ) {
    setFieldError(form, "delta_max_ratio", "比例必须位于 1% 到 100% 之间。");
    valid = false;
  }
  $("#taskFormStatus").textContent = valid ? "" : "请补充必填信息。";
  return valid;
}

async function saveTask(event) {
  event.preventDefault();
  const payload = collectTaskPayload();
  if (!validateTaskPayload(payload)) return;
  const id = $("#taskId").value;
  const button = $("#saveTaskButton");
  setButtonBusy(button, true, "正在保存…");
  $("#taskFormStatus").textContent = "";
  try {
    await api(id ? `/tasks/${encodeURIComponent(id)}` : "/tasks", {
      method: id ? "PUT" : "POST",
      body: payload,
    });
    closeDialog("taskDialog");
    showToast(id ? "任务已更新" : "任务已创建", `${payload.name} 的配置已经生效。`);
    await loadTasks({ silent: true });
    await loadSystem({ silent: true });
  } catch (error) {
    applyFormError($("#taskForm"), error, $("#taskFormStatus"));
  } finally {
    setButtonBusy(button, false);
  }
}

async function toggleTask(taskId, button) {
  const task = taskById(taskId);
  if (!task) return;
  setButtonBusy(button, true);
  try {
    await api(`/tasks/${encodeURIComponent(taskId)}`, {
      method: "PUT",
      body: { enabled: !task.enabled },
    });
    showToast(task.enabled ? "任务已停用" : "任务已启用", task.name);
    await loadTasks({ silent: true });
  } catch (error) {
    showToast("状态修改失败", error.message, "error");
  } finally {
    setButtonBusy(button, false);
  }
}

function askConfirm({ title, message, confirmText = "确认", tone = "danger", action }) {
  $("#confirmTitle").textContent = title;
  $("#confirmMessage").textContent = message;
  $("#confirmIcon").textContent = tone === "danger" ? "!" : "?";
  const button = $("#confirmActionButton");
  button.textContent = confirmText;
  button.className = `button ${tone === "danger" ? "button-danger" : "button-primary"}`;
  state.confirmAction = action;
  openDialog("confirmDialog");
}

async function executeConfirm(event) {
  event.preventDefault();
  if (typeof state.confirmAction !== "function") {
    closeDialog("confirmDialog");
    return;
  }
  const button = $("#confirmActionButton");
  setButtonBusy(button, true);
  try {
    await state.confirmAction();
    state.confirmAction = null;
    closeDialog("confirmDialog");
  } catch (error) {
    showToast("操作失败", error.message, "error");
  } finally {
    setButtonBusy(button, false);
  }
}

function deleteTask(taskId) {
  const task = taskById(taskId);
  if (!task) return;
  askConfirm({
    title: "删除备份任务？",
    message: `将删除“${task.name}”的任务配置。\n若任务有已完成或待对账快照，系统会为保护恢复索引而拒绝删除；可改为停用任务。`,
    confirmText: "删除任务",
    action: async () => {
      await api(`/tasks/${encodeURIComponent(taskId)}`, { method: "DELETE" });
      if (state.snapshotTaskId === taskId) {
        state.snapshotTaskId = "";
        state.snapshots = [];
        resetManifest();
      }
      showToast("任务已删除", task.name);
      await loadTasks({ silent: true });
    },
  });
}

function openRunDialog(taskId) {
  const task = taskById(taskId);
  if (!task) return;
  $("#runTaskId").value = taskId;
  $("#runForceFull").checked = false;
  $("#runDialogDescription").textContent = `“${task.name}”将加入执行队列。`;
  openDialog("runDialog");
}

async function runTask(event) {
  event.preventDefault();
  const taskId = $("#runTaskId").value;
  const task = taskById(taskId);
  const button = $("#confirmRunButton");
  setButtonBusy(button, true, "正在提交…");
  try {
    const operation = await api(`/tasks/${encodeURIComponent(taskId)}/run`, {
      method: "POST",
      body: { force_full: $("#runForceFull").checked },
    });
    closeDialog("runDialog");
    showToast("备份已开始", task?.name ?? "任务已加入队列");
    if (operation) mergeOperation(operation.operation ?? operation);
    await loadOperations({ silent: true });
    renderTasks();
  } catch (error) {
    showToast("无法启动备份", error.message, "error");
  } finally {
    setButtonBusy(button, false);
  }
}

function operationStatsText(operation) {
  const stats = operation.stats ?? {};
  const done = stats.bytes_done ?? stats.uploaded_bytes ?? stats.processed_bytes;
  const total = stats.bytes_total ?? stats.total_bytes;
  const filesDone = stats.files_done ?? stats.processed_files ?? stats.file_count;
  const speed = stats.speed_bps;
  const parts = [];
  if (done !== undefined) parts.push(total ? `${formatBytes(done)} / ${formatBytes(total)}` : formatBytes(done));
  if (filesDone !== undefined) parts.push(`${filesDone} 个文件`);
  if (speed) parts.push(`${formatBytes(speed)}/s`);
  return parts.join(" · ");
}

function operationProgress(operation) {
  const value =
    typeof operation.progress === "object"
      ? operation.progress?.percent ?? operation.progress?.progress
      : operation.progress;
  const number = Number(value);
  return Number.isFinite(number) ? Math.min(100, Math.max(0, number)) : null;
}

function renderProgressTrack(operation) {
  const track = element("div", "progress-track");
  const fill = element("div", "progress-fill");
  const progress = operationProgress(operation);
  if (progress === null && ACTIVE_STATUSES.has(operation.status)) {
    fill.classList.add("indeterminate");
  } else {
    fill.style.width = `${progress ?? (operation.status === "completed" ? 100 : 0)}%`;
  }
  track.append(fill);
  return track;
}

function operationSortValue(operation) {
  return new Date(operation.created_at ?? operation.started_at ?? 0).getTime();
}

function renderOperationCard(operation) {
  const card = element("article", "operation-card");
  const title = element("div", "operation-title");
  const kind = String(operation.kind ?? "backup");
  const kindMark = element("span", `operation-kind ${kind}`, {
    backup: "⇧",
    restore: "⇩",
    scrub: "◎",
    prune: "⌫",
  }[kind] ?? "↻");
  const titleCopy = element("div");
  const task = taskById(operation.task_id);
  append(
    titleCopy,
    element("h3", "", `${operationKindLabel(kind)} · ${task?.name ?? truncate(operation.task_id, 15)}`),
    element("p", "", `#${truncate(operation.id, 14)}`),
  );
  append(title, kindMark, titleCopy);

  const progressWrap = element("div", "operation-progress");
  const progressHead = element("div", "operation-progress-head");
  const progress = operationProgress(operation);
  append(
    progressHead,
    element("span", "", operation.message || phaseLabel(operation.phase)),
    element("strong", "", progress === null ? phaseLabel(operation.phase) : `${Math.round(progress)}%`),
  );
  append(progressWrap, progressHead, renderProgressTrack(operation));
  const statsText = operationStatsText(operation);
  if (statsText) {
    const meta = element("div", "progress-meta");
    append(meta, element("span", "", statsText), element("span", "", formatDuration(operation.started_at)));
    progressWrap.append(meta);
  }

  const time = element("div", "operation-time");
  append(
    time,
    element("strong", "", formatDate(operation.started_at ?? operation.created_at)),
    element("p", "", operation.completed_at ? `耗时 ${formatDuration(operation.started_at, operation.completed_at)}` : formatRelative(operation.created_at)),
  );

  const action = element("div", "operation-action");
  if (ACTIVE_STATUSES.has(operation.status)) {
    const cancel = element("button", "button button-secondary small", operation.status === "cancelling" ? "取消中…" : "取消");
    cancel.type = "button";
    cancel.dataset.action = "cancel-operation";
    cancel.dataset.operationId = operation.id;
    cancel.disabled = operation.status === "cancelling";
    action.append(cancel);
  } else {
    action.append(statusBadge(operation.status, operationStatusLabel(operation.status)));
  }

  append(card, title, progressWrap, time, action);
  if (operation.error) card.append(element("p", "operation-error", operation.error));
  return card;
}

function filteredOperations() {
  const filter = state.operationFilter;
  return [...state.operations]
    .filter((operation) => {
      if (filter === "active") return ACTIVE_STATUSES.has(operation.status);
      if (filter === "completed") return ["completed", "succeeded"].includes(operation.status);
      if (filter === "failed") return operation.status === "failed";
      return true;
    })
    .sort((a, b) => operationSortValue(b) - operationSortValue(a));
}

function renderOperations() {
  const container = $("#operationList");
  clear(container);
  const operations = filteredOperations();
  if (!state.operations.length) {
    createEmptyState(container, "暂无运行记录", "运行备份或发起还原后，实时进度会出现在这里。");
  } else if (!operations.length) {
    createEmptyState(container, "当前筛选没有记录", "选择其他状态查看历史操作。");
  } else {
    operations.forEach((operation) => container.append(renderOperationCard(operation)));
  }

  const active = state.operations.filter((operation) => ACTIVE_STATUSES.has(operation.status));
  $("#metricRunning").textContent = active.length;
  $("#metricRunningHint").textContent = active.length ? "可在运行记录中取消" : "当前队列空闲";
  $("#navOperationPulse").classList.toggle("is-hidden", active.length === 0);

  const completed = [...state.operations]
    .filter((operation) => ["completed", "succeeded"].includes(operation.status))
    .sort((a, b) => operationSortValue(b) - operationSortValue(a))[0];
  $("#metricSuccess").textContent = completed ? formatRelative(completed.completed_at ?? completed.created_at) : "暂无";
  $("#metricSuccessHint").textContent = completed
    ? `${operationKindLabel(completed.kind)} · ${taskById(completed.task_id)?.name ?? "未知任务"}`
    : "运行首次备份后显示";

  renderDashboardOperations();
  renderTasks();
  renderActivityDock();
  renderSidebarOverview();
  updateHeroMessage();
}

function renderDashboardOperations() {
  const container = $("#dashboardOperations");
  clear(container);
  const active = [...state.operations]
    .filter((operation) => ACTIVE_STATUSES.has(operation.status))
    .sort((a, b) => operationSortValue(b) - operationSortValue(a))
    .slice(0, 3);
  if (!active.length) {
    createEmptyState(container, "当前没有运行中的操作", "系统处于空闲状态，可以立即运行任一备份任务。");
    return;
  }

  for (const operation of active) {
    const card = element("article", "live-operation");
    const head = element("div", "live-operation-head");
    const copy = element("div");
    const task = taskById(operation.task_id);
    append(
      copy,
      element("h3", "", `${operationKindLabel(operation.kind)} · ${task?.name ?? "未知任务"}`),
      element("p", "", operation.message || phaseLabel(operation.phase)),
    );
    append(head, copy, statusBadge(operation.status, operationStatusLabel(operation.status)));
    const meta = element("div", "progress-meta");
    const progress = operationProgress(operation);
    append(
      meta,
      element("span", "", operationStatsText(operation) || phaseLabel(operation.phase)),
      element("strong", "", progress === null ? "处理中" : `${Math.round(progress)}%`),
    );
    append(card, head, renderProgressTrack(operation), meta);
    container.append(card);
  }
}

function renderActivityDock() {
  const dock = $("#activityDock");
  const active = [...state.operations]
    .filter((operation) => ACTIVE_STATUSES.has(operation.status))
    .sort((a, b) => operationSortValue(b) - operationSortValue(a));
  const operation = active[0];
  if (!operation) {
    state.dismissedOperationId = "";
    dock.classList.add("is-hidden");
    return;
  }
  if (state.dismissedOperationId === operation.id) {
    dock.classList.add("is-hidden");
    return;
  }

  const task = taskById(operation.task_id);
  const progress = operationProgress(operation);
  const fill = $("#activityDockProgress");
  const progressbar = $(".activity-progress", dock);
  const stats = operation.stats ?? {};
  const done = numberOr(
    stats.bytes_done ?? stats.uploaded_bytes ?? stats.processed_bytes,
    0,
  );
  const total = numberOr(stats.bytes_total ?? stats.total_bytes, 0);
  const speed = numberOr(stats.speed_bps, 0);
  const etaSeconds = speed > 0 && total > done ? Math.ceil((total - done) / speed) : null;
  const extraCount = active.length - 1;
  const statsParts = [
    operationStatsText(operation) || `已运行 ${formatDuration(operation.started_at ?? operation.created_at)}`,
  ];
  if (etaSeconds !== null) {
    statsParts.push(`预计剩余 ${etaSeconds < 60 ? `${etaSeconds} 秒` : `${Math.ceil(etaSeconds / 60)} 分钟`}`);
  }
  if (extraCount) statsParts.push(`另有 ${extraCount} 项`);

  $("#activityDockTitle").textContent =
    `${operationKindLabel(operation.kind)} · ${task?.name ?? truncate(operation.task_id, 22)}`;
  $("#activityDockPhase").textContent = operation.message || phaseLabel(operation.phase);
  $("#activityDockStats").textContent = statsParts.join(" · ");
  $("#activityDockPercent").textContent = progress === null ? "处理中" : `${Math.round(progress)}%`;
  fill.classList.toggle("indeterminate", progress === null);
  fill.style.width = progress === null ? "38%" : `${progress}%`;
  progressbar.setAttribute("aria-valuetext", progress === null ? "处理中" : `${Math.round(progress)}%`);
  if (progress === null) progressbar.removeAttribute("aria-valuenow");
  else progressbar.setAttribute("aria-valuenow", String(Math.round(progress)));
  dock.classList.remove("is-hidden");
}

function updateHeroMessage() {
  const active = state.operations.filter((operation) => ACTIVE_STATUSES.has(operation.status)).length;
  const failed = [...state.operations]
    .sort((a, b) => operationSortValue(b) - operationSortValue(a))
    .find((operation) => operation.status === "failed");
  if (active) {
    $("#heroMessage").textContent = `${active} 个操作正在安全处理数据，进度会在这里实时同步。`;
  } else if (failed) {
    $("#heroMessage").textContent = `当前队列空闲；最近一次失败发生在 ${formatRelative(failed.completed_at ?? failed.created_at)}，建议查看运行记录。`;
  } else if (state.tasks.length) {
    const enabled = state.tasks.filter((task) => task.enabled).length;
    $("#heroMessage").textContent = `当前队列空闲，${enabled} 个自动任务正在按计划守护数据。`;
  } else {
    $("#heroMessage").textContent = "从创建第一个备份任务开始，为重要文件建立可靠的恢复点。";
  }
}

async function loadOperations({ silent = false } = {}) {
  try {
    state.operations = unwrapList(await api("/operations"));
    renderOperations();
    return state.operations;
  } catch (error) {
    if (!state.operations.length) renderOperations();
    if (!silent) showToast("运行记录读取失败", error.message, "error");
    return state.operations;
  }
}

function mergeOperation(incoming) {
  if (!incoming || typeof incoming !== "object") return;
  const id = incoming.id ?? incoming.operation_id;
  if (!id) return;
  const index = state.operations.findIndex((operation) => operation.id === id);
  const previous = index >= 0 ? state.operations[index] : { id };
  const progressObject =
    typeof incoming.progress === "object" && incoming.progress !== null
      ? incoming.progress
      : null;
  const merged = {
    ...previous,
    ...incoming,
    id,
    ...(progressObject
      ? {
          progress: progressObject.percent ?? progressObject.progress ?? previous.progress,
          phase: progressObject.phase ?? incoming.phase ?? previous.phase,
          message: progressObject.message ?? incoming.message ?? previous.message,
          stats: { ...(previous.stats ?? {}), ...(progressObject.stats ?? {}), ...(incoming.stats ?? {}) },
        }
      : {
          stats: { ...(previous.stats ?? {}), ...(incoming.stats ?? {}) },
        }),
  };
  if (index >= 0) state.operations[index] = merged;
  else state.operations.unshift(merged);
  renderOperations();
}

function cancelOperation(operationId) {
  const operation = state.operations.find((item) => item.id === operationId);
  if (!operation) return;
  askConfirm({
    title: "取消正在执行的操作？",
    message: "服务会在当前安全检查点停止，并清理未完成的资源。已完成的分片不会被误标为成功。",
    confirmText: "请求取消",
    tone: "primary",
    action: async () => {
      const response = await api(`/operations/${encodeURIComponent(operationId)}/cancel`, { method: "POST" });
      mergeOperation(response?.operation ?? response ?? { id: operationId, status: "cancelling" });
      showToast("已请求取消", "操作将在安全检查点停止。", "warning");
    },
  });
}

function resetManifest() {
  state.selectedSnapshotId = "";
  state.manifest = null;
  state.selectedPaths.clear();
  $("#manifestContent").classList.add("is-hidden");
  $("#manifestPlaceholder").classList.remove("is-hidden");
  $("#manifestPlaceholder").replaceChildren(
    element("span", "empty-glyph", "◇"),
    element("h2", "", "选择一个快照"),
    element("p", "", "查看文件清单、筛选目标内容并发起安全还原。"),
  );
}

function ensureSnapshotTaskSelected() {
  if (!state.tasks.length) {
    createEmptyState($("#snapshotList"), "还没有备份任务", "先创建任务并完成一次备份，快照会显示在这里。", "新建任务", "new-task");
    resetManifest();
    return;
  }
  if (!state.snapshotTaskId || !taskById(state.snapshotTaskId)) {
    state.snapshotTaskId = state.tasks[0].id;
    $("#snapshotTaskFilter").value = state.snapshotTaskId;
  }
  loadSnapshots(state.snapshotTaskId, { silent: true });
}

function snapshotSize(snapshot) {
  return snapshot.archive_size ?? snapshot.size_bytes ?? snapshot.total_size ?? 0;
}

function snapshotDate(snapshot) {
  return snapshot.completed_at ?? snapshot.started_at ?? snapshot.created_at;
}

function renderSnapshotList() {
  const container = $("#snapshotList");
  clear(container);
  if (!state.snapshotTaskId) {
    createEmptyState(container, "请选择备份任务", "选择任务后加载其历史快照。");
    return;
  }
  if (!state.snapshots.length) {
    createEmptyState(container, "暂无可用快照", "运行一次备份后，即可在这里浏览和恢复文件。");
    return;
  }
  const snapshots = [...state.snapshots].sort(
    (a, b) => new Date(snapshotDate(b) ?? 0) - new Date(snapshotDate(a) ?? 0),
  );
  for (const snapshot of snapshots) {
    const button = element(
      "button",
      `snapshot-item${snapshot.id === state.selectedSnapshotId ? " is-active" : ""}`,
    );
    button.type = "button";
    button.dataset.action = "select-snapshot";
    button.dataset.snapshotId = snapshot.id;
    const head = element("div", "snapshot-item-head");
    const kind = snapshot.kind ?? "full";
    append(
      head,
      element("h3", "", formatDate(snapshotDate(snapshot))),
      element("span", `snapshot-kind ${kind}`, kind === "full" ? "全量" : "增量"),
    );
    const meta = element("div", "snapshot-item-meta");
    append(
      meta,
      element("span", "", `${snapshot.file_count ?? 0} 个文件`),
      element("span", "", formatBytes(snapshotSize(snapshot))),
    );
    append(button, head, meta);
    container.append(button);
  }
  renderSidebarOverview();
}

async function loadSnapshots(taskId, { silent = false } = {}) {
  if (!taskId) {
    state.snapshots = [];
    renderSnapshotList();
    resetManifest();
    return [];
  }
  state.snapshotTaskId = taskId;
  $("#snapshotTaskFilter").value = taskId;
  showLoading($("#snapshotList"), 3);
  try {
    state.snapshots = unwrapList(await api(`/snapshots?task_id=${encodeURIComponent(taskId)}`));
    if (!state.snapshots.some((snapshot) => snapshot.id === state.selectedSnapshotId)) {
      state.selectedSnapshotId = "";
      state.manifest = null;
      state.selectedPaths.clear();
    }
    renderSnapshotList();
    if (state.snapshots.length && !state.selectedSnapshotId) {
      await selectSnapshot(state.snapshots[0].id);
    } else if (!state.snapshots.length) {
      resetManifest();
    }
    return state.snapshots;
  } catch (error) {
    state.snapshots = [];
    renderSnapshotList();
    resetManifest();
    if (!silent) showToast("快照读取失败", error.message, "error");
    return [];
  }
}

async function selectSnapshot(snapshotId) {
  const snapshot = snapshotById(snapshotId);
  if (!snapshot) return;
  state.selectedSnapshotId = snapshotId;
  state.manifest = null;
  state.selectedPaths.clear();
  renderSnapshotList();
  const placeholder = $("#manifestPlaceholder");
  placeholder.classList.remove("is-hidden");
  placeholder.replaceChildren(
    element("span", "empty-glyph", "↻"),
    element("h2", "", "正在读取文件清单"),
    element("p", "", "大型快照可能需要几秒钟。"),
  );
  $("#manifestContent").classList.add("is-hidden");
  try {
    state.manifest = await api(`/snapshots/${encodeURIComponent(snapshotId)}/manifest`);
    renderManifest();
  } catch (error) {
    state.manifest = null;
    placeholder.replaceChildren(
      element("span", "empty-glyph", "!"),
      element("h2", "", "无法读取文件清单"),
      element("p", "", error.message),
    );
    showToast("Manifest 读取失败", error.message, "error");
  }
}

function manifestFiles() {
  return Array.isArray(state.manifest?.files) ? state.manifest.files : [];
}

function filteredManifestFiles() {
  const query = $("#manifestSearch").value.trim().toLocaleLowerCase("zh-CN");
  if (!query) return manifestFiles();
  return manifestFiles().filter((file) =>
    String(file.path ?? "").toLocaleLowerCase("zh-CN").includes(query),
  );
}

function renderManifest() {
  const snapshot = snapshotById(state.selectedSnapshotId);
  if (!state.manifest || !snapshot) return;
  $("#manifestPlaceholder").classList.add("is-hidden");
  $("#manifestContent").classList.remove("is-hidden");
  $("#manifestTitle").textContent = `${snapshot.kind === "full" ? "全量" : "增量"}快照 · ${formatDate(snapshotDate(snapshot))}`;
  const files = manifestFiles();
  const archiveSize =
    state.manifest.archive_integrity?.size ??
    state.manifest.archives?.reduce((sum, archive) => sum + numberOr(archive.integrity?.size, 0), 0) ??
    snapshotSize(snapshot);
  $("#manifestMeta").textContent = `${files.length} 个文件 · ${formatBytes(archiveSize)} · 链 ${truncate(snapshot.chain_id, 16)}`;
  $("#manifestSearch").value = "";
  state.selectedPaths.clear();
  renderManifestFiles();
}

function renderManifestFiles() {
  const body = $("#manifestFileList");
  clear(body);
  const filtered = filteredManifestFiles();
  const limit = 500;
  const visible = filtered.slice(0, limit);
  if (!visible.length) {
    const row = element("tr");
    const cell = element(
      "td",
      "",
      manifestFiles().length ? "没有匹配的文件" : "快照中没有文件",
    );
    cell.colSpan = 6;
    cell.style.textAlign = "center";
    cell.style.padding = "30px";
    row.append(cell);
    body.append(row);
  } else {
    for (const file of visible) {
      const row = element("tr");
      const checkCell = element("td", "check-column");
      const check = element("input");
      check.type = "checkbox";
      check.dataset.filePath = file.path;
      check.checked = state.selectedPaths.has(file.path);
      check.setAttribute("aria-label", `选择 ${file.path}`);
      checkCell.append(check);
      const pathCell = element("td", "file-path");
      append(pathCell, element("span", "file-icon", "▱"), element("span", "", file.path));
      const version = file.file_version;
      const modeCell = element("td", "file-mode");
      let versionBadge;
      if (version?.kind === "delta") {
        versionBadge = element("span", "version-badge delta", "xdelta3 差分");
        const baseSnapshot = snapshotById(version.base?.snapshot_id);
        const baseLabel = baseSnapshot
          ? formatDate(snapshotDate(baseSnapshot), false)
          : truncate(version.base?.snapshot_id, 18);
        const ratio =
          version.original_size > 0
            ? Math.max(0, Math.round((1 - version.transfer_size / version.original_size) * 100))
            : null;
        versionBadge.title = `依赖基线：${baseLabel}`;
        append(
          modeCell,
          versionBadge,
          element(
            "small",
            "version-meta",
            `${formatBytes(version.transfer_size)} Patch${ratio === null ? "" : ` · 节省 ${ratio}%`}`,
          ),
        );
      } else if (version?.kind === "full") {
        versionBadge = element("span", "version-badge full", "全量 Base");
        versionBadge.title = "可独立还原的完整大文件基线";
        append(modeCell, versionBadge, element("small", "version-meta", `${formatBytes(version.transfer_size)} 传输包`));
      } else {
        versionBadge = element("span", "version-badge standard", "普通归档");
        versionBadge.title = "随快照归档存储";
        modeCell.append(versionBadge);
      }
      const versionSnapshot = snapshotById(version?.snapshot_id);
      const backupDate = snapshotDate(versionSnapshot ?? snapshotById(state.selectedSnapshotId));
      const actionCell = element("td", "file-row-actions");
      const restoreButton = element("button", "table-restore-button", version?.kind === "delta" ? "合成还原" : "还原");
      restoreButton.type = "button";
      restoreButton.dataset.action = "restore-file";
      restoreButton.dataset.filePath = file.path;
      restoreButton.setAttribute("aria-label", `还原 ${file.path}`);
      actionCell.append(restoreButton);
      append(
        row,
        checkCell,
        pathCell,
        modeCell,
        element("td", "file-size", formatBytes(version?.original_size ?? file.size)),
        element("td", "file-backup-date", formatDate(backupDate, false)),
        actionCell,
      );
      body.append(row);
    }
  }

  $("#manifestLimitNote").textContent =
    filtered.length > limit
      ? `当前显示前 ${limit} 项，共 ${filtered.length} 项；继续输入路径可缩小范围。`
      : `当前显示 ${filtered.length} 项。`;
  updateManifestSelection();
}

function updateManifestSelection() {
  const filtered = filteredManifestFiles();
  const selectedFiltered = filtered.filter((file) => state.selectedPaths.has(file.path)).length;
  const selectAll = $("#manifestSelectAll");
  selectAll.checked = filtered.length > 0 && selectedFiltered === filtered.length;
  selectAll.indeterminate = selectedFiltered > 0 && selectedFiltered < filtered.length;
  $("#manifestSelectionCount").textContent = `已选择 ${state.selectedPaths.size} 项`;
}

function openRestoreDialog() {
  if (!state.selectedSnapshotId || !state.manifest) {
    showToast("请先选择快照", "读取文件清单后才能发起还原。", "warning");
    return;
  }
  clearFormErrors($("#restoreForm"));
  $("#restoreAll").checked = state.selectedPaths.size === 0;
  $("#restoreOverwrite").value = "skip";
  $("#restoreVerify").checked = true;
  updateRestoreSummary();
  openDialog("restoreDialog");
  window.setTimeout(() => $("#restoreDestination").focus(), 20);
}

function updateRestoreSummary() {
  const restoreAll = $("#restoreAll").checked;
  const selectedFiles = restoreAll
    ? manifestFiles()
    : manifestFiles().filter((file) => state.selectedPaths.has(file.path));
  $("#restoreSelectionSummary").textContent = restoreAll
    ? `将还原快照中的全部 ${selectedFiles.length} 个文件。`
    : `将还原已选择的 ${state.selectedPaths.size} 个文件。`;

  const deltaFiles = selectedFiles.filter((file) => file.file_version?.kind === "delta");
  const note = $("#restoreDependencyNote");
  if (!deltaFiles.length) {
    note.classList.add("is-hidden");
    note.textContent = "";
    return;
  }
  if (deltaFiles.length === 1 && selectedFiles.length === 1) {
    const baseId = deltaFiles[0].file_version?.base?.snapshot_id;
    const baseSnapshot = snapshotById(baseId);
    const baseLabel = baseSnapshot ? formatDate(snapshotDate(baseSnapshot), false) : truncate(baseId, 18);
    note.textContent =
      `该文件将使用 ${baseLabel} 的完整 Base + 当前 xdelta3 Patch 单步合成；完成后会强制校验 SHA-256。`;
  } else {
    note.textContent =
      `其中 ${deltaFiles.length} 个文件将自动获取各自的完整 Base 与当前 xdelta3 Patch，单步合成后统一校验 SHA-256。`;
  }
  note.classList.remove("is-hidden");
}

async function submitRestore(event) {
  event.preventDefault();
  const form = $("#restoreForm");
  clearFormErrors(form);
  const destination = $("#restoreDestination").value.trim();
  const restoreAll = $("#restoreAll").checked;
  if (!destination) {
    setFieldError(form, "destination_path", "请输入还原目标目录。");
    return;
  }
  if (!restoreAll && state.selectedPaths.size === 0) {
    showToast("未选择文件", "请选择文件，或开启“还原整个快照”。", "warning");
    return;
  }
  const button = $("#confirmRestoreButton");
  setButtonBusy(button, true, "正在提交…");
  try {
    const response = await api("/restores", {
      method: "POST",
      body: {
        snapshot_id: state.selectedSnapshotId,
        destination_path: destination,
        paths: restoreAll ? [] : [...state.selectedPaths],
        restore_all: restoreAll,
        overwrite: $("#restoreOverwrite").value,
        verify: $("#restoreVerify").checked,
      },
    });
    closeDialog("restoreDialog");
    $("#restoreDestination").value = "";
    showToast("还原已开始", `文件将恢复到 ${destination}`);
    if (response) mergeOperation(response.operation ?? response);
    await loadOperations({ silent: true });
    navigate("operations");
  } catch (error) {
    applyFormError(form, error);
  } finally {
    setButtonBusy(button, false);
  }
}

function openScrubDialog() {
  if (!state.selectedSnapshotId) {
    showToast("请先选择快照", "", "warning");
    return;
  }
  $("#scrubDeep").checked = false;
  $("#scrubRatio").value = "1";
  $("#scrubRatioOutput").textContent = "1%";
  $("#scrubRatioField").classList.remove("is-hidden");
  openDialog("scrubDialog");
}

async function submitScrub(event) {
  event.preventDefault();
  const button = event.submitter;
  setButtonBusy(button, true, "正在提交…");
  try {
    const deep = $("#scrubDeep").checked;
    const response = await api(`/snapshots/${encodeURIComponent(state.selectedSnapshotId)}/scrub`, {
      method: "POST",
      body: {
        deep,
        sample_ratio: deep ? 1 : numberOr($("#scrubRatio").value, 1) / 100,
      },
    });
    closeDialog("scrubDialog");
    showToast("巡检已开始", deep ? "正在执行完整校验。" : `将抽样校验 ${$("#scrubRatio").value}% 的数据。`);
    if (response) mergeOperation(response.operation ?? response);
    await loadOperations({ silent: true });
    navigate("operations");
  } catch (error) {
    showToast("无法启动巡检", error.message, "error");
  } finally {
    setButtonBusy(button, false);
  }
}

function renderCredentials() {
  const container = $("#credentialList");
  clear(container);
  if (!state.credentials.length) {
    createEmptyState(container, "尚未保存凭据", "添加 S3 / OSS 访问密钥后，即可在备份任务中引用。", "添加凭据", "new-credential");
  } else {
    for (const credential of state.credentials) {
      const card = element("article", "credential-card");
      const head = element("div", "credential-card-head");
      const mark = element("span", "credential-glyph", "⌘");
      const headActions = element("div", "credential-card-actions");
      const rotateButton = element("button", "text-button", "轮换");
      rotateButton.type = "button";
      rotateButton.dataset.action = "rotate-credential";
      rotateButton.dataset.profile = credential.profile;
      rotateButton.setAttribute("aria-label", `轮换凭据 ${credential.profile}`);
      const deleteButton = element("button", "icon-button", "×");
      deleteButton.type = "button";
      deleteButton.dataset.action = "delete-credential";
      deleteButton.dataset.profile = credential.profile;
      deleteButton.setAttribute("aria-label", `删除凭据 ${credential.profile}`);
      append(headActions, rotateButton, deleteButton);
      append(head, mark, headActions);
      const tokenBadge = credential.has_session_token
        ? statusBadge("warning", "含临时令牌")
        : statusBadge("completed", "已安全保存");
      const footer = element("footer");
      append(
        footer,
        element("span", "", `${credential.backend ?? "系统密钥环"} · ${formatDate(credential.updated_at)}`),
        tokenBadge,
      );
      append(
        card,
        head,
        element("h2", "", credential.profile),
        element("p", "", credential.access_key_hint || "AK ****"),
        footer,
      );
      container.append(card);
    }
  }
  populateTaskOptions();
  renderSidebarOverview();
}

async function loadCredentials({ silent = false } = {}) {
  try {
    state.credentials = unwrapList(await api("/credentials"));
    renderCredentials();
    return state.credentials;
  } catch (error) {
    state.credentials = [];
    renderCredentials();
    if (!silent) showToast("凭据读取失败", error.message, "error");
    return [];
  }
}

function openCredentialForm(profile = "") {
  const form = $("#credentialForm");
  form.reset();
  clearFormErrors(form);
  $("#credentialSecretKey").type = "password";
  $("#toggleSecretButton").textContent = "显示";
  $("#credentialDialogTitle").textContent = profile ? "轮换 S3 凭据" : "添加 S3 凭据";
  $("#credentialProfile").value = profile;
  $("#credentialProfile").readOnly = Boolean(profile);
  openDialog("credentialDialog");
  window.setTimeout(() => (profile ? $("#credentialAccessKey") : $("#credentialProfile")).focus(), 20);
}

async function saveCredential(event) {
  event.preventDefault();
  const form = $("#credentialForm");
  clearFormErrors(form);
  const payload = {
    profile: $("#credentialProfile").value.trim(),
    access_key_id: $("#credentialAccessKey").value.trim(),
    secret_access_key: $("#credentialSecretKey").value,
    session_token: $("#credentialSessionToken").value.trim() || null,
  };
  let valid = true;
  for (const [field, message] of [
    ["profile", "请输入配置名称。"],
    ["access_key_id", "请输入 Access Key ID。"],
    ["secret_access_key", "请输入 Secret Access Key。"],
  ]) {
    if (!payload[field]) {
      setFieldError(form, field, message);
      valid = false;
    }
  }
  if (!valid) return;

  const button = $("#saveCredentialButton");
  setButtonBusy(button, true, "正在安全保存…");
  try {
    await api("/credentials", { method: "POST", body: payload });
    const profile = payload.profile;
    form.reset();
    closeDialog("credentialDialog");
    showToast("凭据已保存", `${profile} 已写入系统安全存储。`);
    await loadCredentials({ silent: true });
  } catch (error) {
    applyFormError(form, error);
  } finally {
    setButtonBusy(button, false);
  }
}

function deleteCredential(profile) {
  askConfirm({
    title: "删除凭据？",
    message: `将从系统安全存储中删除“${profile}”。\n若当前任务、可恢复快照或待对账快照仍在引用，系统会拒绝删除。`,
    confirmText: "删除凭据",
    action: async () => {
      await api(`/credentials/${encodeURIComponent(profile)}`, { method: "DELETE" });
      showToast("凭据已删除", profile);
      await loadCredentials({ silent: true });
    },
  });
}

function refreshCurrentView() {
  const button = $("#refreshButton");
  setButtonBusy(button, true, "刷新中…");
  const loaders = [loadSystem({ silent: true })];
  if (["dashboard", "tasks"].includes(state.view)) loaders.push(loadTasks({ silent: true }));
  if (["dashboard", "operations"].includes(state.view)) loaders.push(loadOperations({ silent: true }));
  if (state.view === "snapshots" && state.snapshotTaskId) {
    loaders.push(loadSnapshots(state.snapshotTaskId, { silent: true }));
  }
  if (state.view === "credentials") loaders.push(loadCredentials({ silent: true }));
  Promise.allSettled(loaders).finally(() => {
    setButtonBusy(button, false);
    showToast("已刷新", "当前页面已同步最新状态。");
  });
}

function scheduleResync() {
  window.clearTimeout(state.resyncTimer);
  state.resyncTimer = window.setTimeout(async () => {
    await Promise.allSettled([
      loadOperations({ silent: true }),
      loadTasks({ silent: true }),
      loadSystem({ silent: true }),
    ]);
    if (state.snapshotTaskId) loadSnapshots(state.snapshotTaskId, { silent: true });
  }, 700);
}

function handleSocketMessage(messageEvent) {
  let event;
  try {
    event = JSON.parse(messageEvent.data);
  } catch {
    return;
  }
  const type = String(event.type ?? "");
  const data = event.data ?? {};
  if (type === "hello" || type === "pong") return;

  if (type.startsWith("operation.")) {
    const incoming = data.operation ?? data;
    const normalized = { ...incoming };
    if (!normalized.id && normalized.operation_id) normalized.id = normalized.operation_id;
    if (TERMINAL_EVENT_STATUSES[type] && !normalized.status) {
      normalized.status = TERMINAL_EVENT_STATUSES[type];
    }
    if (type === "operation.started" && !normalized.status) normalized.status = "running";
    mergeOperation(normalized);
    if (TERMINAL_EVENT_STATUSES[type]) scheduleResync();
    return;
  }
  if (type.startsWith("task.")) {
    window.clearTimeout(state.resyncTimer);
    state.resyncTimer = window.setTimeout(() => loadTasks({ silent: true }), 250);
    return;
  }
  if (type.startsWith("snapshot.")) {
    if (state.snapshotTaskId) {
      window.clearTimeout(state.resyncTimer);
      state.resyncTimer = window.setTimeout(
        () => loadSnapshots(state.snapshotTaskId, { silent: true }),
        350,
      );
    }
    return;
  }
  if (type === "system.health" || type.startsWith("system.")) {
    state.system = { ...(state.system ?? {}), ...data };
    renderSystem();
  }
}

function connectSocket() {
  window.clearTimeout(state.reconnectTimer);
  setConnection(state.reconnectAttempt ? "offline" : "connecting");
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const socketUrl = `${protocol}//${window.location.host}${API_BASE}/ws`;
  let socket;
  try {
    socket = new WebSocket(socketUrl);
  } catch {
    scheduleReconnect();
    return;
  }
  state.ws = socket;

  socket.addEventListener("open", () => {
    state.reconnectAttempt = 0;
    setConnection("online");
    Promise.allSettled([
      loadOperations({ silent: true }),
      loadTasks({ silent: true }),
      loadSystem({ silent: true }),
    ]);
  });
  socket.addEventListener("message", handleSocketMessage);
  socket.addEventListener("close", scheduleReconnect);
  socket.addEventListener("error", () => {
    socket.close();
  });
}

function scheduleReconnect() {
  if (document.visibilityState === "hidden") {
    setConnection("offline");
  }
  state.ws = null;
  state.reconnectAttempt += 1;
  setConnection("offline");
  const base = Math.min(30_000, 1000 * 2 ** Math.min(state.reconnectAttempt - 1, 5));
  const delay = base + Math.round(Math.random() * 500);
  window.clearTimeout(state.reconnectTimer);
  state.reconnectTimer = window.setTimeout(connectSocket, delay);
}

function handleBodyClick(event) {
  const closeControl = event.target.closest("[data-close-dialog]");
  if (closeControl) {
    closeDialog(closeControl.dataset.closeDialog);
    return;
  }

  const navigateControl = event.target.closest("[data-navigate]");
  if (navigateControl) {
    navigate(navigateControl.dataset.navigate);
    return;
  }

  const actionControl = event.target.closest("[data-action]");
  if (!actionControl) return;
  const action = actionControl.dataset.action;
  if (action === "new-task") openTaskForm();
  else if (action === "edit-task") openTaskForm(actionControl.dataset.taskId);
  else if (action === "delete-task") deleteTask(actionControl.dataset.taskId);
  else if (action === "toggle-task") toggleTask(actionControl.dataset.taskId, actionControl);
  else if (action === "run-task") openRunDialog(actionControl.dataset.taskId);
  else if (action === "view-snapshots") {
    state.snapshotTaskId = actionControl.dataset.taskId;
    $("#snapshotTaskFilter").value = state.snapshotTaskId;
    navigate("snapshots");
  } else if (action === "cancel-operation") {
    cancelOperation(actionControl.dataset.operationId);
  } else if (action === "select-snapshot") {
    selectSnapshot(actionControl.dataset.snapshotId);
  } else if (action === "restore-file") {
    state.selectedPaths.clear();
    state.selectedPaths.add(actionControl.dataset.filePath);
    renderManifestFiles();
    openRestoreDialog();
  } else if (action === "refresh-operations") {
    loadOperations();
  } else if (action === "refresh-snapshots") {
    if (state.snapshotTaskId) loadSnapshots(state.snapshotTaskId);
  } else if (action === "refresh-system") {
    loadSystem().then(() => showToast("检测完成", "系统组件状态已更新。"));
  } else if (action === "new-credential") {
    openCredentialForm();
  } else if (action === "rotate-credential") {
    openCredentialForm(actionControl.dataset.profile);
  } else if (action === "delete-credential") {
    deleteCredential(actionControl.dataset.profile);
  }
}

function bindEvents() {
  $$(".nav-item").forEach((button) => {
    button.addEventListener("click", () => navigate(button.dataset.view));
  });
  document.body.addEventListener("click", handleBodyClick);
  $("#menuButton").addEventListener("click", () => {
    if ($("#sidebar").classList.contains("is-open")) closeMobileMenu();
    else openMobileMenu();
  });
  $("#mobileScrim").addEventListener("click", closeMobileMenu);
  $("#refreshButton").addEventListener("click", refreshCurrentView);
  $("#globalNewTaskButton").addEventListener("click", () => openTaskForm());

  $("#taskSearch").addEventListener("input", (event) => {
    state.taskFilter.query = event.target.value;
    renderTasks();
  });
  $("#taskStatusFilter").addEventListener("change", (event) => {
    state.taskFilter.status = event.target.value;
    renderTasks();
  });
  $$(`input[name="storage_kind"]`).forEach((radio) => {
    radio.addEventListener("change", () => {
      toggleStorageFields(radio.value);
    });
  });
  $("#taskForm").addEventListener("input", (event) => {
    if (event.target.matches(STORAGE_CONFIG_FIELD_SELECTOR)) {
      invalidateStorageProbe();
    }
  });
  $("#s3Endpoint").addEventListener("blur", () => {
    normalizeS3EndpointInput({ showError: true });
  });
  $("#taskSchedulePreset").addEventListener("change", (event) => {
    applySchedulePreset(event.target.value);
    if (event.target.value === "custom") $("#taskSchedule").focus();
  });
  $("#taskFullEvery").addEventListener("input", updateFullEveryPresets);
  $$("[data-full-every]").forEach((button) => {
    button.addEventListener("click", () => {
      $("#taskFullEvery").value = button.dataset.fullEvery;
      updateFullEveryPresets();
    });
  });
  $("#testStorageButton").addEventListener("click", testStorageConfiguration);
  $("#taskDeltaEnabled").addEventListener("change", (event) => {
    toggleDeltaFields(event.target.checked);
  });
  $("#taskForm").addEventListener("submit", saveTask);
  $("#runForm").addEventListener("submit", runTask);
  $("#confirmForm").addEventListener("submit", executeConfirm);

  $$("#operationFilters button").forEach((button) => {
    button.addEventListener("click", () => {
      state.operationFilter = button.dataset.operationFilter;
      $$("#operationFilters button").forEach((item) => item.classList.toggle("is-active", item === button));
      renderOperations();
    });
  });

  $("#snapshotTaskFilter").addEventListener("change", (event) => {
    state.snapshotTaskId = event.target.value;
    state.selectedSnapshotId = "";
    state.manifest = null;
    state.selectedPaths.clear();
    loadSnapshots(state.snapshotTaskId);
  });
  $("#manifestSearch").addEventListener("input", renderManifestFiles);
  $("#manifestSelectAll").addEventListener("change", (event) => {
    const files = filteredManifestFiles();
    for (const file of files) {
      if (event.target.checked) state.selectedPaths.add(file.path);
      else state.selectedPaths.delete(file.path);
    }
    renderManifestFiles();
  });
  $("#manifestFileList").addEventListener("change", (event) => {
    if (!event.target.matches(`input[type="checkbox"][data-file-path]`)) return;
    if (event.target.checked) state.selectedPaths.add(event.target.dataset.filePath);
    else state.selectedPaths.delete(event.target.dataset.filePath);
    updateManifestSelection();
  });
  $("#restoreSnapshotButton").addEventListener("click", openRestoreDialog);
  $("#restoreAll").addEventListener("change", updateRestoreSummary);
  $("#restoreForm").addEventListener("submit", submitRestore);
  $("#scrubSnapshotButton").addEventListener("click", openScrubDialog);
  $("#scrubRatio").addEventListener("input", (event) => {
    $("#scrubRatioOutput").textContent = `${event.target.value}%`;
  });
  $("#scrubDeep").addEventListener("change", (event) => {
    $("#scrubRatioField").classList.toggle("is-hidden", event.target.checked);
  });
  $("#scrubForm").addEventListener("submit", submitScrub);

  $("#credentialForm").addEventListener("submit", saveCredential);
  $("#authForm").addEventListener("submit", submitAuthentication);
  $("#authDialog").addEventListener("cancel", (event) => {
    event.preventDefault();
  });
  $("#toggleSecretButton").addEventListener("click", () => {
    const input = $("#credentialSecretKey");
    const showing = input.type === "text";
    input.type = showing ? "password" : "text";
    $("#toggleSecretButton").textContent = showing ? "显示" : "隐藏";
    $("#toggleSecretButton").setAttribute("aria-label", showing ? "显示密钥" : "隐藏密钥");
  });
  $("#activityDockDismiss").addEventListener("click", () => {
    const active = [...state.operations]
      .filter((operation) => ACTIVE_STATUSES.has(operation.status))
      .sort((a, b) => operationSortValue(b) - operationSortValue(a))[0];
    state.dismissedOperationId = active?.id ?? "";
    renderActivityDock();
  });
  $("#activityDockOpen").addEventListener("click", () => navigate("operations"));

  window.addEventListener("hashchange", () => {
    navigate(window.location.hash.replace(/^#\//, "") || "dashboard", false);
  });
  window.addEventListener("online", () => {
    if (!state.ws || state.ws.readyState > 1) connectSocket();
  });
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && (!state.ws || state.ws.readyState > 1)) connectSocket();
  });
  window.addEventListener("beforeunload", () => {
    window.clearTimeout(state.reconnectTimer);
    if (state.ws) state.ws.close();
  });
}

async function initialize() {
  bindEvents();
  const initialView = window.location.hash.replace(/^#\//, "") || "dashboard";
  navigate(initialView, false);
  showLoading($("#dashboardOperations"), 2);
  showLoading($("#dashboardTasks"), 2);
  showLoading($("#dashboardTools"), 3);
  showLoading($("#taskList"), 4);
  showLoading($("#operationList"), 3);
  showLoading($("#credentialList"), 3);

  await Promise.allSettled([
    loadSystem({ silent: true }),
    loadTasks({ silent: true }),
    loadOperations({ silent: true }),
    loadCredentials({ silent: true }),
  ]);
  renderTasks();
  renderOperations();
  renderCredentials();
  renderSystem();
  if (state.view === "snapshots") ensureSnapshotTaskSelected();
  updateHeroMessage();
  connectSocket();

  window.setInterval(() => {
    if (state.wsState !== "online" || state.operations.some((operation) => ACTIVE_STATUSES.has(operation.status))) {
      loadOperations({ silent: true });
    }
  }, 15_000);
  window.setInterval(() => loadSystem({ silent: true }), 60_000);
}

initialize();
