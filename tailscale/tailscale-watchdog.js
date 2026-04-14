#!/usr/bin/env node
"use strict";

// ================================================================
// tailscale/tailscale-watchdog.js
//
// Monitoring-first watchdog for Tailscale.
//
// Default behavior (safe):
//   - Monitor only (NO auto-heal)
//   - Logs structured warnings with root-cause hints
//
// Optional heal mode:
//   - Reconnect via `tailscale up`
//   - Patch resolv.conf if magic DNS IP is missing
//
// Environment variables:
//   TAILSCALE_WATCHDOG_MODE            monitor|heal (default: monitor)
//   TAILSCALE_WATCHDOG_INTERVAL_SEC    check interval in seconds (default: 30)
//   TAILSCALE_WATCHDOG_ALERT_EVERY     repeat warning every N failed cycles (default: 5)
//   TAILSCALE_WATCHDOG_LOG_OK_EVERY    log healthy status every N cycles, 0=always (default: 10)
//   TAILSCALE_WATCHDOG_NETCHECK        include localapi netcheck in warnings (default: true)
//
//   TAILSCALE_WATCHDOG_AUTO_RECONNECT  true|false (default: false in monitor, true in heal)
//   TAILSCALE_WATCHDOG_DNS_CHECK       true|false (default: true)
//   TAILSCALE_WATCHDOG_DNS_CHECK_ALWAYS true|false (default: false)
//   TAILSCALE_WATCHDOG_DNS_FIX         true|false (default: false in monitor, true in heal)
//   TAILSCALE_WATCHDOG_DNS_FIX_ALWAYS  true|false (default: false)
//   TAILSCALE_WATCHDOG_RECONNECT_MIN_SEC  min seconds between reconnect attempts (default: 60)
//   TAILSCALE_WATCHDOG_HEAL_AFTER_STREAK  failed cycles before reconnect attempt (default: 2)
//   TAILSCALE_WATCHDOG_UP_ACCEPT_DNS      pass --accept-dns to tailscale up (default: false)
//
//   TAILSCALE_SOCKET                   local API socket path (default: /tmp/tailscaled.sock)
//   TAILSCALE_BIN                      tailscale binary (default: tailscale)
//   TAILSCALE_DNS_MAGIC_IP             magic DNS IP (default: 100.100.100.100)
//   TAILSCALE_RESOLV_CONF              resolv.conf path (default: /etc/resolv.conf)
//   TAILSCALE_UP_EXTRA_ARGS            extra args for `tailscale up` (default: "")
//   PROJECT_NAME                       host label in logs
// ================================================================

const http = require("http");
const fs = require("fs");
const { execFile } = require("child_process");

function toBool(value, fallback) {
  if (value === undefined || value === null || value === "") return fallback;
  const v = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(v)) return true;
  if (["0", "false", "no", "n", "off"].includes(v)) return false;
  return fallback;
}

function toIntMin(value, fallback, min) {
  const n = parseInt(value, 10);
  return Number.isFinite(n) ? Math.max(min, n) : fallback;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function nowLabel() {
  return new Date().toISOString().replace("T", " ").slice(0, 19);
}

const ICON = { error: "ERR", warn: "WRN", ok: "OK", info: "INF" };

function safeJson(details) {
  try {
    return JSON.stringify(details);
  } catch {
    return '{"_error":"cannot_serialize_details"}';
  }
}

function logEvent(level, code, message, details) {
  const icon = ICON[level] || ICON.info;
  const base = `[${nowLabel()}] ${icon} [watchdog][${code}] ${message}`;
  if (details && typeof details === "object" && Object.keys(details).length > 0) {
    console.log(`${base} | ${safeJson(details)}`);
    return;
  }
  console.log(base);
}

function localApiRequest({ socketPath, method = "GET", path, body, timeoutMs = 8000 }) {
  return new Promise((resolve, reject) => {
    const payload = body !== undefined ? JSON.stringify(body) : undefined;
    const headers = {
      Accept: "application/json",
      // Tailscale localapi requires a localapi host header on unix socket requests.
      Host: "local-tailscaled.sock",
    };
    if (payload) {
      headers["Content-Type"] = "application/json";
      headers["Content-Length"] = Buffer.byteLength(payload);
    }

    const req = http.request({ socketPath, method, path, headers }, (res) => {
      clearTimeout(timer);
      let raw = "";
      res.on("data", (chunk) => {
        raw += chunk;
      });
      res.on("end", () => {
        let parsedBody = null;
        try {
          parsedBody = raw ? JSON.parse(raw) : null;
        } catch {
          parsedBody = null;
        }
        resolve({ status: res.statusCode || 0, body: parsedBody, raw });
      });
    });

    const timer = setTimeout(() => {
      req.destroy(new Error(`local API timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    req.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });

    if (payload) req.write(payload);
    req.end();
  });
}

async function getLocalApiJson(socketPath, path) {
  const res = await localApiRequest({
    socketPath,
    method: "GET",
    path,
  });
  if (res.status !== 200 || !res.body) return null;
  return res.body;
}

async function getTailscaleStatus(socketPath) {
  try {
    return await getLocalApiJson(socketPath, "/localapi/v0/status");
  } catch (err) {
    throw new Error(`local API /status unreachable: ${err.message}`);
  }
}

async function getTailscaleNetcheck(socketPath) {
  try {
    return await getLocalApiJson(socketPath, "/localapi/v0/netcheck");
  } catch (err) {
    throw new Error(`local API /netcheck unreachable: ${err.message}`);
  }
}

function execTailscaleUp({ bin, socketPath, extraArgs, timeoutMs = 35000 }) {
  const args = ["--socket", socketPath, "up", ...extraArgs].filter(Boolean);
  return new Promise((resolve) => {
    execFile(bin, args, { timeout: timeoutMs }, (err, stdout, stderr) => {
      resolve({
        ok: !err,
        err: err ? err.message : null,
        stdout: (stdout || "").trim(),
        stderr: (stderr || "").trim(),
      });
    });
  });
}

function readNameserversFromText(content) {
  const nameservers = [];
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (trimmed.startsWith("nameserver ")) {
      nameservers.push(trimmed.slice("nameserver ".length).trim());
    }
  }
  return nameservers;
}

function inspectResolvConf(resolvConfPath, magicIp) {
  try {
    const content = fs.readFileSync(resolvConfPath, "utf-8");
    const nameservers = readNameserversFromText(content);
    return {
      ok: true,
      nameservers,
      hasMagicIp: nameservers.includes(magicIp),
    };
  } catch (err) {
    return {
      ok: false,
      nameservers: [],
      hasMagicIp: false,
      error: err.message,
    };
  }
}

const WATCHDOG_TAG = "# tailscale-watchdog";

function ensureMagicIpInResolvConf(resolvConfPath, magicIp) {
  const inspected = inspectResolvConf(resolvConfPath, magicIp);
  if (!inspected.ok) {
    logEvent("error", "TSWD_DNS_READ_FAIL", "cannot read resolv.conf", {
      resolvConfPath,
      error: inspected.error,
    });
    return false;
  }
  if (inspected.hasMagicIp) return false;

  let current = "";
  try {
    current = fs.readFileSync(resolvConfPath, "utf-8");
  } catch (err) {
    logEvent("error", "TSWD_DNS_READ_FAIL", "cannot read resolv.conf", {
      resolvConfPath,
      error: err.message,
    });
    return false;
  }

  const filtered = current
    .split(/\r?\n/)
    .filter((l) => !l.includes(WATCHDOG_TAG))
    .join("\n");
  const prepended = `nameserver ${magicIp} ${WATCHDOG_TAG}\n${filtered}`;

  try {
    fs.writeFileSync(resolvConfPath, prepended, "utf-8");
    logEvent("ok", "TSWD_DNS_FIXED", "prepended magic DNS IP into resolv.conf", {
      resolvConfPath,
      magicIp,
    });
    return true;
  } catch (err) {
    logEvent("warn", "TSWD_DNS_FIX_FAIL", "cannot write resolv.conf", {
      resolvConfPath,
      magicIp,
      error: err.message,
    });
    return false;
  }
}

function arraysEqual(a, b) {
  if (a.length !== b.length) return false;
  return a.every((v, i) => v === b[i]);
}

function pickFirst(obj, keys, fallback = null) {
  if (!obj || typeof obj !== "object") return fallback;
  for (const key of keys) {
    if (obj[key] !== undefined && obj[key] !== null) return obj[key];
  }
  return fallback;
}

function summarizePeerStatus(status) {
  const peersObj = status && typeof status.Peer === "object" && status.Peer ? status.Peer : {};
  const peers = Object.values(peersObj);
  let online = 0;
  let activeRelay = 0;
  for (const peer of peers) {
    if (peer && peer.Online === true) online += 1;
    if (peer && peer.Relay) activeRelay += 1;
  }
  const total = peers.length;
  return {
    total,
    online,
    offline: Math.max(total - online, 0),
    withRelay: activeRelay,
  };
}

function summarizeNetcheck(netcheck) {
  if (!netcheck || typeof netcheck !== "object") return null;
  return {
    udp: pickFirst(netcheck, ["UDP", "udp"], null),
    ipv4: pickFirst(netcheck, ["IPv4", "ipv4"], null),
    ipv6: pickFirst(netcheck, ["IPv6", "ipv6"], null),
    hairpin: pickFirst(netcheck, ["HairPinning", "hairPinning"], null),
    mappingVariesByDestIP: pickFirst(netcheck, ["MappingVariesByDestIP", "mappingVariesByDestIP"], null),
    preferredDerp: pickFirst(netcheck, ["PreferredDERP", "preferredDERP", "PreferredDerp"], null),
    nearestDerp: pickFirst(netcheck, ["NearestDERP", "nearestDERP", "NearestDerp"], null),
  };
}

function inferLikelyCauses({ backendState, online, healthItems, dnsMissing }) {
  const causes = [];

  if (backendState === "NeedsLogin") causes.push("needs_login");
  if (backendState === "NeedsMachineAuth") causes.push("needs_machine_auth");
  if (backendState === "Stopped") causes.push("daemon_stopped_or_not_joined");
  if (backendState === "Starting") causes.push("daemon_starting_or_recovering");
  if (online === false) causes.push("self_offline_on_control_plane");

  const healthText = (healthItems || []).join(" | ").toLowerCase();
  if (healthText.includes("derp") || healthText.includes("relay")) causes.push("derp_or_relay_path_issue");
  if (healthText.includes("dns")) causes.push("dns_configuration_issue");
  if (healthText.includes("udp")) causes.push("udp_blocked_or_unstable");
  if (dnsMissing) causes.push("magic_dns_ip_missing_from_resolv_conf");

  if (!causes.length) causes.push("unknown_check_health_and_netcheck");
  return [...new Set(causes)];
}

function extractDerpHome(status) {
  const relay = status?.Self?.Relay;
  if (relay && typeof relay === "string") return relay;
  return null;
}

function extractSelfIdentity(status) {
  const self = status && typeof status.Self === "object" && status.Self ? status.Self : {};
  const tailnet =
    status && typeof status.CurrentTailnet === "object" && status.CurrentTailnet ? status.CurrentTailnet : {};
  return {
    nodeId: pickFirst(self, ["ID", "NodeID", "NodeId", "IDHex"], null),
    dnsName: pickFirst(self, ["DNSName", "DnsName"], null),
    hostName: pickFirst(self, ["HostName", "Hostname", "Name"], null),
    tailnetName: pickFirst(tailnet, ["Name", "Tailnet"], null),
    magicDnsSuffix: pickFirst(tailnet, ["MagicDNSSuffix", "MagicDnsSuffix"], null),
  };
}

function selfIdentityFingerprint(self) {
  return JSON.stringify([
    self?.nodeId || "",
    self?.dnsName || "",
    self?.hostName || "",
    self?.tailnetName || "",
    self?.magicDnsSuffix || "",
  ]);
}

async function runHealthCheck({
  mode,
  socketPath,
  bin,
  extraArgs,
  resolvConfPath,
  magicIp,
  alertEvery,
  netcheckEnabled,
  autoReconnect,
  dnsCheck,
  dnsCheckAlways,
  dnsFix,
  dnsFixAlways,
  reconnectMinMs,
  healAfterStreak,
  state,
}) {
  const result = {
    statusOk: false,
    backendState: null,
    online: false,
    ips: [],
    derpHome: null,
    self: null,
    peerSummary: null,
    reconnectAttempted: false,
    reconnectOk: null,
    dnsPatch: false,
  };

  let status;
  try {
    status = await getTailscaleStatus(socketPath);
  } catch (err) {
    state.socketFailures += 1;
    if (state.socketFailures === 1 || state.socketFailures % alertEvery === 0) {
      logEvent("error", "TSWD_SOCKET_UNREACHABLE", "tailscaled local API is unreachable", {
        socketPath,
        failureCount: state.socketFailures,
        error: err.message,
      });
      logEvent("warn", "TSWD_SOCKET_HINT", "verify tailscaled is running and socket path is correct", {
        socketPath,
      });
    }
    return result;
  }
  state.socketFailures = 0;

  if (!status) {
    logEvent("warn", "TSWD_STATUS_EMPTY", "local API returned empty /status payload");
    return result;
  }

  result.backendState = status.BackendState || "Unknown";
  result.online = status.Self?.Online === true;
  result.ips = Array.isArray(status.Self?.TailscaleIPs) ? status.Self.TailscaleIPs : [];
  result.derpHome = extractDerpHome(status);
  result.self = extractSelfIdentity(status);
  result.peerSummary = summarizePeerStatus(status);
  result.statusOk = true;

  const healthItems = Array.isArray(status.Health) ? status.Health : [];
  const selfFingerprint = selfIdentityFingerprint(result.self);
  const hasIdentityInfo =
    result.self &&
    typeof result.self === "object" &&
    Object.values(result.self).some((v) => v !== null && v !== "");
  if (hasIdentityInfo && selfFingerprint !== state.lastSelfIdentityFingerprint) {
    logEvent("info", "TSWD_SELF_IDENTITY", "observed tailscale self identity", {
      ...result.self,
      hint: "match nodeId/dnsName with Tailscale admin to avoid stale hostname confusion",
    });
    state.lastSelfIdentityFingerprint = selfFingerprint;
  }

  if (healthItems.length > 0 && !arraysEqual(healthItems, state.lastHealth)) {
    healthItems.forEach((msg) => {
      logEvent("warn", "TSWD_HEALTH_WARN", "tailscaled reported health warning", {
        message: msg,
      });
    });
    state.lastHealth = healthItems;
  } else if (healthItems.length === 0 && state.lastHealth.length > 0) {
    logEvent("ok", "TSWD_HEALTH_CLEAR", "tailscaled health warnings cleared");
    state.lastHealth = [];
  }

  const isRunning = result.backendState === "Running" && result.online;

  if (result.backendState !== state.lastBackendState || result.online !== state.lastOnline) {
    logEvent(isRunning ? "ok" : "warn", "TSWD_STATE_CHANGE", "tailscale state changed", {
      previousBackendState: state.lastBackendState || "(init)",
      backendState: result.backendState,
      previousOnline: state.lastOnline,
      online: result.online,
      ips: result.ips,
      derpHome: result.derpHome,
      self: result.self,
      peers: result.peerSummary,
    });
    state.lastBackendState = result.backendState;
    state.lastOnline = result.online;
  }

  if (!result.derpHome && result.online) {
    logEvent("warn", "TSWD_DERP_MISSING", "Self.Online is true but no DERP home relay is reported", {
      backendState: result.backendState,
      online: result.online,
      peers: result.peerSummary,
    });
  }

  if (!isRunning) {
    state.notRunningStreak += 1;
    const shouldWarn = state.notRunningStreak === 1 || state.notRunningStreak % alertEvery === 0;
    if (shouldWarn) {
      let netcheckSummary = null;
      if (netcheckEnabled) {
        try {
          const netcheck = await getTailscaleNetcheck(socketPath);
          netcheckSummary = summarizeNetcheck(netcheck);
        } catch (err) {
          netcheckSummary = { error: err.message };
        }
      }

      const dnsInspection = dnsCheck ? inspectResolvConf(resolvConfPath, magicIp) : null;
      const dnsMissing = dnsInspection && dnsInspection.ok && !dnsInspection.hasMagicIp;
      const likelyCauses = inferLikelyCauses({
        backendState: result.backendState,
        online: result.online,
        healthItems,
        dnsMissing,
      });

      logEvent("warn", "TSWD_NOT_RUNNING", "tailscale is not fully online", {
        streak: state.notRunningStreak,
        backendState: result.backendState,
        online: result.online,
        ips: result.ips,
        derpHome: result.derpHome,
        self: result.self,
        peers: result.peerSummary,
        health: healthItems,
        dns: dnsInspection
          ? {
              checked: true,
              ok: dnsInspection.ok,
              hasMagicIp: dnsInspection.hasMagicIp,
              nameservers: dnsInspection.nameservers,
              error: dnsInspection.error || null,
            }
          : { checked: false },
        netcheck: netcheckSummary,
        likelyCauses,
        action: mode === "monitor" ? "monitor_only_no_heal" : "heal_mode_enabled",
      });
    }
  } else if (state.notRunningStreak > 0) {
    logEvent("ok", "TSWD_RECOVERED", "tailscale returned to fully online state", {
      previousNotRunningStreak: state.notRunningStreak,
      backendState: result.backendState,
      online: result.online,
      ips: result.ips,
      derpHome: result.derpHome,
      self: result.self,
    });
    state.notRunningStreak = 0;
  }

  if (!isRunning && autoReconnect) {
    const needsAuth = result.backendState === "NeedsLogin" || result.backendState === "NeedsMachineAuth";
    if (needsAuth) {
      if (!state.authLoggedOnce) {
        logEvent("error", "TSWD_NEEDS_AUTH", "manual re-authentication is required", {
          backendState: result.backendState,
          hint: "check auth key / OAuth credentials",
        });
        state.authLoggedOnce = true;
      }
    } else if (state.notRunningStreak < healAfterStreak) {
      if (state.notRunningStreak === 1 || state.notRunningStreak % alertEvery === 0) {
        logEvent("info", "TSWD_RECONNECT_WAIT", "waiting before auto-heal attempt", {
          streak: state.notRunningStreak,
          healAfterStreak,
          backendState: result.backendState,
          online: result.online,
        });
      }
    } else {
      const now = Date.now();
      const elapsed = now - (state.lastReconnectAt || 0);
      if (elapsed > reconnectMinMs) {
        result.reconnectAttempted = true;
        state.lastReconnectAt = now;
        logEvent("info", "TSWD_RECONNECT_ATTEMPT", "attempting `tailscale up`", {
          backendState: result.backendState,
          online: result.online,
        });
        const upResult = await execTailscaleUp({ bin, socketPath, extraArgs });
        result.reconnectOk = upResult.ok;
        if (upResult.ok) {
          state.authLoggedOnce = false;
          logEvent("ok", "TSWD_RECONNECT_OK", "`tailscale up` completed", {
            stdout: upResult.stdout || null,
          });
        } else {
          logEvent("error", "TSWD_RECONNECT_FAIL", "`tailscale up` failed", {
            error: upResult.err,
            stderr: upResult.stderr || null,
          });
        }
      }
    }
  } else if (!isRunning && mode === "monitor") {
    const shouldLog = state.notRunningStreak === 1 || state.notRunningStreak % alertEvery === 0;
    if (shouldLog) {
      logEvent("info", "TSWD_MONITOR_ONLY", "auto-reconnect is disabled by monitor-only mode");
    }
  } else if (isRunning) {
    state.authLoggedOnce = false;
  }

  if (dnsCheck && (isRunning || dnsCheckAlways || dnsFixAlways)) {
    const inspected = inspectResolvConf(resolvConfPath, magicIp);
    if (!inspected.ok) {
      state.dnsReadFailStreak += 1;
      if (state.dnsReadFailStreak === 1 || state.dnsReadFailStreak % alertEvery === 0) {
        logEvent("warn", "TSWD_DNS_READ_FAIL", "cannot read resolv.conf for DNS diagnostics", {
          streak: state.dnsReadFailStreak,
          resolvConfPath,
          error: inspected.error,
        });
      }
    } else {
      if (state.dnsReadFailStreak > 0) {
        logEvent("ok", "TSWD_DNS_READ_RECOVERED", "resolv.conf is readable again", {
          previousStreak: state.dnsReadFailStreak,
          resolvConfPath,
        });
        state.dnsReadFailStreak = 0;
      }

      if (!inspected.hasMagicIp) {
        state.dnsMissingStreak += 1;
        if (state.dnsMissingStreak === 1 || state.dnsMissingStreak % alertEvery === 0) {
          logEvent("warn", "TSWD_DNS_MAGIC_MISSING", "magic DNS IP is missing from resolv.conf", {
            streak: state.dnsMissingStreak,
            resolvConfPath,
            magicIp,
            nameservers: inspected.nameservers,
            action: dnsFix ? "will_try_patch" : "monitor_only_no_patch",
          });
        }
        if (dnsFix && (isRunning || dnsFixAlways)) {
          result.dnsPatch = ensureMagicIpInResolvConf(resolvConfPath, magicIp);
        }
      } else if (state.dnsMissingStreak > 0) {
        logEvent("ok", "TSWD_DNS_MAGIC_RECOVERED", "magic DNS IP is present again", {
          previousStreak: state.dnsMissingStreak,
          resolvConfPath,
          magicIp,
          nameservers: inspected.nameservers,
        });
        state.dnsMissingStreak = 0;
      }
    }
  }

  return result;
}

async function watchdogLoop(cfg) {
  const { intervalMs, logOkEvery, projectName } = cfg;

  const state = {
    socketFailures: 0,
    lastBackendState: "",
    lastOnline: null,
    lastHealth: [],
    lastReconnectAt: 0,
    authLoggedOnce: false,
    notRunningStreak: 0,
    lastSelfIdentityFingerprint: "",
    dnsMissingStreak: 0,
    dnsReadFailStreak: 0,
    tick: 0,
  };

  logEvent("info", "TSWD_START", "watchdog started", {
    host: projectName || "(unknown)",
    mode: cfg.mode,
    intervalSec: intervalMs / 1000,
    alertEvery: cfg.alertEvery,
    logOkEvery: cfg.logOkEvery,
    socketPath: cfg.socketPath,
    autoReconnect: cfg.autoReconnect,
    dnsCheck: cfg.dnsCheck,
    dnsFix: cfg.dnsFix,
    reconnectMinSec: cfg.reconnectMinMs / 1000,
    healAfterStreak: cfg.healAfterStreak,
    upAcceptDns: cfg.upAcceptDns,
    magicIp: cfg.magicIp,
    resolvConfPath: cfg.resolvConfPath,
  });

  while (true) {
    state.tick += 1;

    try {
      const res = await runHealthCheck({ ...cfg, state });

      if (res.statusOk && res.backendState === "Running" && res.online) {
        if (logOkEvery <= 0 || state.tick % logOkEvery === 0) {
          logEvent("ok", "TSWD_HEALTHY", "tailscale is healthy", {
            state: res.backendState,
            online: res.online,
            ips: res.ips,
            derpHome: res.derpHome,
            self: res.self,
            peers: res.peerSummary,
          });
        }
      }
    } catch (err) {
      logEvent("error", "TSWD_CYCLE_ERROR", "unexpected error in watchdog cycle", {
        error: err.message,
      });
    }

    await sleep(intervalMs);
  }
}

function normalizeMode(value) {
  const mode = String(value || "monitor").trim().toLowerCase();
  if (mode === "heal") return "heal";
  return "monitor";
}

async function run() {
  const mode = normalizeMode(process.env.TAILSCALE_WATCHDOG_MODE);
  const intervalSec = toIntMin(process.env.TAILSCALE_WATCHDOG_INTERVAL_SEC, 30, 10);
  const alertEvery = toIntMin(process.env.TAILSCALE_WATCHDOG_ALERT_EVERY, 5, 1);
  const logOkEvery = toIntMin(process.env.TAILSCALE_WATCHDOG_LOG_OK_EVERY, 10, 0);
  const netcheckEnabled = toBool(process.env.TAILSCALE_WATCHDOG_NETCHECK, true);

  const autoReconnectDefault = mode === "heal";
  const dnsFixDefault = mode === "heal";
  let autoReconnect = toBool(process.env.TAILSCALE_WATCHDOG_AUTO_RECONNECT, autoReconnectDefault);
  let dnsFix = toBool(process.env.TAILSCALE_WATCHDOG_DNS_FIX, dnsFixDefault);

  if (mode === "monitor") {
    if (autoReconnect) {
      logEvent(
        "warn",
        "TSWD_CFG_OVERRIDE",
        "TAILSCALE_WATCHDOG_AUTO_RECONNECT=true ignored because mode=monitor",
      );
    }
    if (dnsFix) {
      logEvent(
        "warn",
        "TSWD_CFG_OVERRIDE",
        "TAILSCALE_WATCHDOG_DNS_FIX=true ignored because mode=monitor",
      );
    }
    autoReconnect = false;
    dnsFix = false;
  }

  const dnsCheck = toBool(process.env.TAILSCALE_WATCHDOG_DNS_CHECK, true);
  const dnsCheckAlways = toBool(process.env.TAILSCALE_WATCHDOG_DNS_CHECK_ALWAYS, false);
  const dnsFixAlways = toBool(process.env.TAILSCALE_WATCHDOG_DNS_FIX_ALWAYS, false);
  const reconnectMinSec = toIntMin(process.env.TAILSCALE_WATCHDOG_RECONNECT_MIN_SEC, 60, 10);
  const healAfterStreak = toIntMin(process.env.TAILSCALE_WATCHDOG_HEAL_AFTER_STREAK, 2, 1);

  const socketPath = (process.env.TAILSCALE_SOCKET || "/tmp/tailscaled.sock").trim();
  const bin = (process.env.TAILSCALE_BIN || "tailscale").trim();
  const magicIp = (process.env.TAILSCALE_DNS_MAGIC_IP || "100.100.100.100").trim();
  const resolvConfPath = (process.env.TAILSCALE_RESOLV_CONF || "/etc/resolv.conf").trim();
  const projectName = (process.env.PROJECT_NAME || "").trim();
  const upAcceptDns = toBool(process.env.TAILSCALE_WATCHDOG_UP_ACCEPT_DNS, false);
  const extraArgsRaw = (process.env.TAILSCALE_UP_EXTRA_ARGS || "").trim();
  const extraArgs = [
    `--accept-dns=${upAcceptDns ? "true" : "false"}`,
    ...(extraArgsRaw ? extraArgsRaw.split(/\s+/).filter(Boolean) : []),
  ];

  const stop = (signal) => {
    logEvent("info", "TSWD_STOP", `received ${signal}, stopping watchdog`);
    process.exit(0);
  };
  process.on("SIGINT", () => stop("SIGINT"));
  process.on("SIGTERM", () => stop("SIGTERM"));

  await watchdogLoop({
    mode,
    intervalMs: intervalSec * 1000,
    alertEvery,
    logOkEvery,
    netcheckEnabled,
    autoReconnect,
    dnsCheck,
    dnsCheckAlways,
    dnsFix,
    dnsFixAlways,
    reconnectMinMs: reconnectMinSec * 1000,
    healAfterStreak,
    socketPath,
    bin,
    upAcceptDns,
    magicIp,
    resolvConfPath,
    extraArgs,
    projectName,
  });
}

run().catch((err) => {
  console.error(`[${nowLabel()}] ERR [watchdog][TSWD_FATAL] ${err.message}`);
  process.exit(1);
});
