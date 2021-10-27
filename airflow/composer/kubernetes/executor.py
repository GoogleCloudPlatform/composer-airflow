import logging
import os
import tempfile

from airflow.configuration import AIRFLOW_HOME, conf

POD_TEMPLATE_FILE = os.path.join(AIRFLOW_HOME, "composer_kubernetes_pod_template_file.yaml")
POD_TEMPLATE_FILE_REFRESH_INTERVAL = conf.getint(
    "kubernetes", "pod_template_file_refresh_interval", fallback=60
)

log = logging.getLogger(__file__)


def refresh_pod_template_file():
    """Refreshes Composer pod template file used by KubernetesExecutor."""
    log.info("Refreshing Composer kubernetes pod template file")

    # TODO: temporary use dummy spec, replace it with real pod spec from GKE.
    pod_spec = """---
kind: Pod
apiVersion: v1
metadata:
  name: dummy-name-dont-delete
  namespace: dummy-name-dont-delete
  labels:
    mylabel: foo
spec:
  containers:
    - name: base
      image: dummy-name-dont-delete
"""

    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write(pod_spec)
    # Atomically override file. "os.rename" is bulletproof to race conditions such as another thread/process
    # will read file while current is overriding it (file handle will continue to refer to the original
    # version of the file).
    # https://stackoverflow.com/questions/2028874/what-happens-to-an-open-file-handle-on-linux-if-the-pointed-file-gets-moved-or-d
    os.rename(f.name, POD_TEMPLATE_FILE)
