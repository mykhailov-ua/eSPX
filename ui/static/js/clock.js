(function (global) {
  "use strict";

  function pad(n) {
    return (n < 10 ? "0" : "") + n;
  }

  function formatLocal(d) {
    return pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
  }

  // Server fan-out stamps labels in UTC (now.UTC().Format).
  function utcHHMMSSToLocal(str) {
    if (!str) return str;
    var m = String(str).match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
    if (!m) return str;
    var d = new Date();
    d.setUTCHours(parseInt(m[1], 10), parseInt(m[2], 10), parseInt(m[3] || "0", 10), 0);
    return formatLocal(d);
  }

  function tick() {
    var now = formatLocal(new Date());
    var nodes = document.querySelectorAll("[data-live-clock]");
    for (var i = 0; i < nodes.length; i++) {
      nodes[i].textContent = now;
    }
    var utcNodes = document.querySelectorAll("[data-utc-hms]");
    for (var j = 0; j < utcNodes.length; j++) {
      utcNodes[j].textContent = utcHHMMSSToLocal(utcNodes[j].getAttribute("data-utc-hms"));
    }
  }

  global.AppClock = {
    formatLocal: formatLocal,
    utcHHMMSSToLocal: utcHHMMSSToLocal,
    tick: tick,
  };

  document.addEventListener("DOMContentLoaded", function () {
    tick();
    setInterval(tick, 1000);
  });
})(window);
