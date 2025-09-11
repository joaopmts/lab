
from IPython.display import display, HTML
from IPython import get_ipython

STYLE = """
<style>
.jp-OutputArea-output pre,
.jp-RenderedText pre,
pre {
  white-space: pre !important;
  overflow: auto !important;
}

.jp-OutputArea-output {
  overflow: auto !important;
  max-height: 60vh;
}
</style>
"""

def _inject_css_once(*_args, **_kwargs):
    ip = get_ipython()
    if not ip or getattr(ip, "_custom_css_injected", False):
        return
    display(HTML(STYLE))
    ip._custom_css_injected = True

ip = get_ipython()
if ip:
    try:
        ip.events.unregister("post_run_cell", _inject_css_once)
    except Exception:
        pass
    ip.events.register("post_run_cell", _inject_css_once)

