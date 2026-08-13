"""
Composer kubernetes patch.

Changes:
- implement pod_mutation_hook for KubernetesPodOperator and KubernetesExecutor support
- patch PodManager to support running KubernetesPodOperator
- generate and refresh periodically pod template file for KubernetesExecutor support
- append airflow-k8s-worker prefix to names of worker pods running by KubernetesExecutor
"""
