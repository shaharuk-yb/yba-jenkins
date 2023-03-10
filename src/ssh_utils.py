import logging
import os
import subprocess
from utils import K8S_RELEASE_DIR


def get_yw_pod(yw_namespace):
    return "{}-yugaware-0".format(yw_namespace)


def run_cmd_on_k8s_yw(k8s_yw_namespace, cmd_str):
    cmd_list = [
        "kubectl", "exec", "-t", "-n", k8s_yw_namespace,
        get_yw_pod(k8s_yw_namespace), "-c", "yugaware", "--",
        "bash", "-c", cmd_str
    ]
    popen = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = [msg.decode('utf-8') for msg in popen.communicate()]
    if popen.returncode != 0:
        raise RuntimeError("CMD={}, RETCODE={}, OUT={}, ERR={}".format(
          cmd_list, popen.returncode, out, err))
    return out, err


def copy_to_k8s_yw(k8s_yw_namespace, local_path, remote_path):
    cmd = [
        "kubectl", "cp", local_path,
        "{}/{}:{}".format(k8s_yw_namespace, get_yw_pod(k8s_yw_namespace), remote_path),
        "-c", "yugaware"
    ]
    subprocess.check_output(cmd)


def copy_release_to_k8s_yw(yugabyte_version, release_path, k8s_yw_namespace):
    logging.info("Copying release image {} to k8s YW at {}".format(
        release_path, K8S_RELEASE_DIR))
    k8s_dir = os.path.join(K8S_RELEASE_DIR, yugabyte_version)
    # There are cases where a file (not directory) already exists in the
    # location of release, delete it, since we can't create a directory
    # otherwise.
    mkdir_cmd = f"((test -f {k8s_dir} && rm -f {k8s_dir}) || true) && mkdir -p {k8s_dir}"
    logging.info(f"Running cmd: {mkdir_cmd}")
    run_cmd_on_k8s_yw(k8s_yw_namespace, mkdir_cmd)
    copy_to_k8s_yw(k8s_yw_namespace, release_path, k8s_dir)