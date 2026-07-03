(function () {
  "use strict";

  var inflight = new WeakMap();

  document.addEventListener("htmx:beforeRequest", function (evt) {
    var elt = evt.detail.elt;
    if (!elt || !elt.hasAttribute("data-poll")) {
      return;
    }
    if (inflight.get(elt)) {
      evt.preventDefault();
    } else {
      inflight.set(elt, true);
    }
  });

  document.addEventListener("htmx:afterRequest", function (evt) {
    var elt = evt.detail.elt;
    if (elt && elt.hasAttribute("data-poll")) {
      inflight.set(elt, false);
    }
  });

  document.addEventListener("htmx:afterSettle", function () {
    if (window.AppClock) {
      window.AppClock.tick();
    }
  });
})();
