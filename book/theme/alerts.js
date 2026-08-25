// Renders GitHub alert blockquotes ("> [!NOTE]") as callouts, so the same
// markdown reads correctly on GitHub and in the guide.
(function () {
  var LABELS = {
    NOTE: 'Note',
    TIP: 'Tip',
    IMPORTANT: 'Important',
    WARNING: 'Warning',
    CAUTION: 'Caution',
  };

  function render() {
    var quotes = document.querySelectorAll('.content blockquote');
    Array.prototype.forEach.call(quotes, function (quote) {
      var first = quote.querySelector('p');
      if (!first) return;
      var match = first.innerHTML.match(/^\[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*(<br>)?\s*/i);
      if (!match) return;

      var kind = match[1].toUpperCase();
      first.innerHTML = first.innerHTML.slice(match[0].length);
      if (!first.innerHTML.trim()) first.remove();

      var label = document.createElement('p');
      label.className = 'alert-label';
      label.textContent = LABELS[kind];
      quote.insertBefore(label, quote.firstChild);
      quote.classList.add('alert', 'alert-' + kind.toLowerCase());
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', render);
  } else {
    render();
  }
})();
