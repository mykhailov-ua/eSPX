(function (global) {
  "use strict";

  var PAD = { top: 20, right: 88, bottom: 36, left: 56 };
  var CHART_HEIGHT = 260;
  var WINDOW_MS = 48000;
  var LERP = 0.14;
  var AXIS_TICKS = 4;

  function chartArea(host) {
    return host.querySelector(".chart-body") || host;
  }

  function labelToUtcMs(str) {
    var m = String(str).match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
    if (!m) return Date.now();
    var d = new Date();
    d.setUTCHours(parseInt(m[1], 10), parseInt(m[2], 10), parseInt(m[3] || "0", 10), 0);
    return d.getTime();
  }

  function formatLocalMs(ms) {
    if (global.AppClock && global.AppClock.formatLocal) {
      return global.AppClock.formatLocal(new Date(ms));
    }
    var d = new Date(ms);
    function p(n) {
      return (n < 10 ? "0" : "") + n;
    }
    return p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds());
  }

  function minMax(values) {
    var min = Infinity;
    var max = -Infinity;
    for (var i = 0; i < values.length; i++) {
      if (values[i] < min) min = values[i];
      if (values[i] > max) max = values[i];
    }
    if (!isFinite(min) || !isFinite(max)) {
      return { min: 0, max: 1 };
    }
    if (min === max) {
      return { min: min - 1, max: max + 1 };
    }
    var pad = (max - min) * 0.08;
    return { min: min - pad, max: max + pad };
  }

  function formatTick(val) {
    var abs = Math.abs(val);
    if (abs >= 100) return String(Math.round(val));
    if (abs >= 10) return String(Math.round(val));
    if (abs >= 1) return val.toFixed(1);
    return val.toFixed(2);
  }

  function scaleY(value, min, max, height) {
    var range = max - min;
    if (range <= 0) return height / 2;
    return PAD.top + ((max - value) / range) * (height - PAD.top - PAD.bottom);
  }

  function pointX(tMs, nowMs, width) {
    var plotW = width - PAD.left - PAD.right;
    var age = nowMs - tMs;
    if (age < 0) age = 0;
    if (age > WINDOW_MS) return PAD.left - 1;
    return width - PAD.right - (age / WINDOW_MS) * plotW;
  }

  function ensureCanvas(host) {
    var area = chartArea(host);
    var canvas = area.querySelector("canvas");
    if (!canvas) {
      canvas = document.createElement("canvas");
      canvas.className = "chart-canvas";
      canvas.setAttribute("aria-hidden", "true");
      area.insertBefore(canvas, area.firstChild);
    }
    return canvas;
  }

  function ensureLiveLabel(host) {
    var area = chartArea(host);
    var label = area.querySelector("[data-chart-live-label]");
    if (!label) {
      label = document.createElement("span");
      label.className = "chart-live-label font-mono tabular-nums muted";
      label.setAttribute("data-chart-live-label", "");
      label.setAttribute("aria-hidden", "true");
      area.appendChild(label);
    }
    return label;
  }

  function layoutWidth(host) {
    var area = chartArea(host);
    var width = Math.floor(area.clientWidth);
    if (width > 0) return width;
    return Math.floor(host.clientWidth) || 640;
  }

  function prepareCanvas(host) {
    var canvas = ensureCanvas(host);
    var dpr = window.devicePixelRatio || 1;
    var width = layoutWidth(host);
    var height = CHART_HEIGHT;

    canvas.style.width = "100%";
    canvas.style.height = height + "px";
    canvas.style.maxWidth = "100%";
    canvas.width = Math.max(1, Math.floor(width * dpr));
    canvas.height = Math.floor(height * dpr);

    var ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    return { canvas: canvas, ctx: ctx, width: width, height: height };
  }

  function buildPoints(values, labels, nowMs, width) {
    var pts = [];
    var rightX = width - PAD.right;

    for (var i = 0; i < values.length; i++) {
      var ms = labelToUtcMs(labels[i]);
      var x = pointX(ms, nowMs, width);
      if (x < PAD.left - 1) continue;
      pts.push({ x: x, y: values[i] });
    }

    if (values.length) {
      var lastY = values[values.length - 1];
      if (!pts.length) {
        pts.push({ x: rightX, y: lastY });
      } else {
        var tail = pts[pts.length - 1];
        if (Math.abs(tail.x - rightX) > 0.5 || tail.y !== lastY) {
          pts.push({ x: rightX, y: lastY });
        } else {
          tail.x = rightX;
          tail.y = lastY;
        }
      }
    }

    pts.sort(function (a, b) {
      return a.x - b.x;
    });
    return pts;
  }

  function drawGrid(ctx, width, height, min, max, tokens) {
    ctx.strokeStyle = tokens.grid;
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (var i = 0; i < 4; i++) {
      var y = PAD.top + (i / 3) * (height - PAD.top - PAD.bottom);
      ctx.moveTo(PAD.left, y);
      ctx.lineTo(width - PAD.right, y);
    }
    ctx.stroke();

    ctx.fillStyle = tokens.text;
    ctx.font = "12px " + tokens.mono;
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    var prevLabel = null;
    for (var j = 0; j < 4; j++) {
      var val = max - (j / 3) * (max - min);
      var label = formatTick(val);
      if (label === prevLabel) continue;
      prevLabel = label;
      var gy = PAD.top + (j / 3) * (height - PAD.top - PAD.bottom);
      ctx.fillText(label, PAD.left - 10, gy);
    }
  }

  function drawTimeAxis(ctx, width, height, nowMs, tokens) {
    var ly = height - PAD.bottom + 8;
    ctx.fillStyle = tokens.text;
    ctx.font = "11px " + tokens.mono;
    ctx.textBaseline = "top";

    for (var i = 0; i <= AXIS_TICKS; i++) {
      var frac = i / AXIS_TICKS;
      var age = WINDOW_MS * (1 - frac);
      var tMs = nowMs - age;
      var x = pointX(tMs, nowMs, width);
      if (x < PAD.left || x > width - PAD.right) continue;

      var label = formatLocalMs(tMs);
      if (i === 0) {
        ctx.textAlign = "left";
        ctx.fillText(label, PAD.left, ly);
      } else if (i === AXIS_TICKS) {
        continue;
      } else {
        ctx.textAlign = "center";
        ctx.fillText(label, x, ly);
      }
    }
  }

  function drawSeries(ctx, pts, width, height, min, max, tokens) {
    if (!pts.length) return;

    var plotW = width - PAD.left - PAD.right;

    ctx.save();
    ctx.beginPath();
    ctx.rect(PAD.left, PAD.top, plotW, height - PAD.top - PAD.bottom);
    ctx.clip();

    ctx.beginPath();
    for (var i = 0; i < pts.length; i++) {
      var y = scaleY(pts[i].y, min, max, height);
      if (i === 0) ctx.moveTo(pts[i].x, y);
      else ctx.lineTo(pts[i].x, y);
    }
    ctx.strokeStyle = tokens.line;
    ctx.lineWidth = 1.5;
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    ctx.stroke();

    ctx.lineTo(pts[pts.length - 1].x, height - PAD.bottom);
    ctx.lineTo(pts[0].x, height - PAD.bottom);
    ctx.closePath();
    ctx.fillStyle = tokens.fill;
    ctx.globalAlpha = 0.35;
    ctx.fill();
    ctx.globalAlpha = 1;
    ctx.restore();
  }

  function lerpDisplay(state) {
    var target = state.targetValues;
    var display = state.displayValues;
    if (!target || !target.length) return;

    if (!display || display.length !== target.length) {
      state.displayValues = target.slice();
      state.displayMin = state.targetMin;
      state.displayMax = state.targetMax;
      return;
    }

    for (var i = 0; i < target.length; i++) {
      display[i] += (target[i] - display[i]) * LERP;
    }
    state.displayMin += (state.targetMin - state.displayMin) * LERP;
    state.displayMax += (state.targetMax - state.displayMax) * LERP;
  }

  function render(host, nowMs) {
    var state = host._chart;
    if (!state || !state.displayValues || !state.displayValues.length) return;

    ensureLiveLabel(host);
    var layout = prepareCanvas(host);
    var ctx = layout.ctx;
    var width = layout.width;
    var height = layout.height;

    ctx.clearRect(0, 0, width, height);
    drawGrid(ctx, width, height, state.displayMin, state.displayMax, state.tokens);

    var pts = buildPoints(state.displayValues, state.targetLabels, nowMs, width);
    drawSeries(ctx, pts, width, height, state.displayMin, state.displayMax, state.tokens);
    drawTimeAxis(ctx, width, height, nowMs, state.tokens);

    var live = host.querySelector("[data-chart-live-label]");
    if (live) {
      live.textContent = formatLocalMs(nowMs);
    }

    state.canvas = layout.canvas;
    state.ctx = ctx;
    state.width = width;
  }

  function setTargets(host, values, labels, tokens) {
    var bounds = minMax(values);
    var state = host._chart || {};

    state.targetValues = values.slice();
    state.targetLabels = labels.slice();
    state.targetMin = bounds.min;
    state.targetMax = bounds.max;
    state.tokens = tokens;

    if (!state.displayValues || state.displayValues.length !== values.length) {
      state.displayValues = values.slice();
      state.displayMin = bounds.min;
      state.displayMax = bounds.max;
    }

    host._chart = state;
  }

  function startLoop(host) {
    stopLoop(host);

    function frame() {
      if (!host._chart || !host._chart.targetValues) return;
      lerpDisplay(host._chart);
      render(host, Date.now());
      host._chart.rafId = requestAnimationFrame(frame);
    }

    host._chart.rafId = requestAnimationFrame(frame);
  }

  function stopLoop(host) {
    if (host._chart && host._chart.rafId) {
      cancelAnimationFrame(host._chart.rafId);
      host._chart.rafId = null;
    }
  }

  function mount(host, data, tokens) {
    var series = data.series && data.series[0];
    if (!series || !series.values || !series.values.length) return;

    host._chart = host._chart || {};
    host._chart.data = data;
    setTargets(host, series.values, data.labels || [], tokens);
    startLoop(host);

    var handler = function () {
      if (!host._chart || !host._chart.targetValues) return;
      render(host, Date.now());
    };
    host._chart.listeners = { resize: handler };
    window.addEventListener("resize", handler);

    if (typeof ResizeObserver !== "undefined") {
      var area = chartArea(host);
      var ro = new ResizeObserver(handler);
      ro.observe(area);
      host._chart.listeners.ro = ro;
    }
  }

  function update(host, data, tokens) {
    var series = data.series && data.series[0];
    if (!series || !series.values || !series.values.length) return;

    host._chart = host._chart || {};
    host._chart.data = data;
    setTargets(host, series.values, data.labels || [], tokens);

    if (!host._chart.rafId) {
      startLoop(host);
    }
  }

  function destroy(host) {
    if (!host._chart) return;
    stopLoop(host);
    if (host._chart.listeners && host._chart.listeners.resize) {
      window.removeEventListener("resize", host._chart.listeners.resize);
    }
    if (host._chart.listeners && host._chart.listeners.ro) {
      host._chart.listeners.ro.disconnect();
    }
    var canvas = host._chart.canvas;
    if (canvas) {
      var ctx = host._chart.ctx;
      if (ctx) ctx.clearRect(0, 0, canvas.width, canvas.height);
      canvas.width = 0;
      canvas.height = 0;
    }
    delete host._chart;
    host.removeAttribute("data-chart-mounted");
    var area = chartArea(host);
    var stale = area.querySelector("canvas");
    if (stale) stale.remove();
  }

  global.LineChart = { mount: mount, update: update, destroy: destroy };
})(window);
