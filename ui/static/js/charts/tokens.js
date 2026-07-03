(function (global) {
  "use strict";

  function read(root) {
    var style = getComputedStyle(root);
    return {
      line: style.getPropertyValue("--chart-line").trim(),
      grid: style.getPropertyValue("--chart-grid").trim(),
      fill: style.getPropertyValue("--chart-fill-muted").trim(),
      text: style.getPropertyValue("--text-muted").trim(),
      mono: style.getPropertyValue("--font-mono").trim(),
    };
  }

  global.ChartTokens = { read: read };
})(window);
