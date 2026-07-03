(function (global) {
  "use strict";

  function parseData(el) {
    var script = el.querySelector('script.chart-data[type="application/json"]');
    if (script && script.textContent) {
      try {
        return JSON.parse(script.textContent);
      } catch (e) {
        return null;
      }
    }
    var raw = el.getAttribute("data-chart");
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch (e) {
      return null;
    }
  }

  function setData(el, data) {
    var script = el.querySelector('script.chart-data[type="application/json"]');
    if (!script) {
      script = document.createElement("script");
      script.type = "application/json";
      script.className = "chart-data";
      el.insertBefore(script, el.firstChild);
    }
    script.textContent = JSON.stringify(data);
  }

  function mount(el) {
    if (!el || el.hasAttribute("data-chart-mounted")) return;
    var data = parseData(el);
    if (!data) return;

    var tokens = global.ChartTokens.read(el);
    if (data.type === "line" && global.LineChart) {
      global.LineChart.mount(el, data, tokens);
      el.setAttribute("data-chart-mounted", "1");
    }
  }

  function update(el, data) {
    if (!el) return;
    if (data) {
      setData(el, data);
    } else {
      data = parseData(el);
    }
    if (!data) return;

    var tokens = global.ChartTokens.read(el);
    if (data.type === "line" && global.LineChart) {
      if (!el.hasAttribute("data-chart-mounted")) {
        mount(el);
        return;
      }
      global.LineChart.update(el, data, tokens);
    }
  }

  function destroy(el) {
    if (!el) return;
    if (global.LineChart) {
      global.LineChart.destroy(el);
    }
    delete el._chart;
    el.removeAttribute("data-chart-mounted");
  }

  function mountAll(root) {
    var scope = root || document;
    var nodes = scope.querySelectorAll(".chart-host:not([data-chart-mounted])");
    for (var i = 0; i < nodes.length; i++) mount(nodes[i]);
  }

  function destroyAll(root) {
    var scope = root || document;
    var nodes = scope.querySelectorAll(".chart-host[data-chart-mounted]");
    for (var i = 0; i < nodes.length; i++) destroy(nodes[i]);
  }

  global.charts = {
    mount: mount,
    update: update,
    destroy: destroy,
    mountAll: mountAll,
    destroyAll: destroyAll,
  };

  document.addEventListener("DOMContentLoaded", function () {
    mountAll(document);
  });

  document.addEventListener("htmx:beforeSwap", function (evt) {
    if (!evt.detail.target) return;
    destroyAll(evt.detail.target);
  });

  document.addEventListener("htmx:afterSettle", function (evt) {
    var target = evt.detail.target;
    if (target) mountAll(target);
  });

  document.addEventListener("dashboardChart", function (evt) {
    var host = document.getElementById("dashboard-chart-host");
    if (host && evt.detail) {
      update(host, evt.detail);
    }
  });
})(window);
