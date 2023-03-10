# Copyright (c) YugaByte, Inc.

import atexit
import glob
import logging
import os
import platform
import re
import shutil
import subprocess
import tempfile
import kubernetes
from utils import BUILDS_DIR, try_command, get_release_os
from ybops.utils import fetch_and_release_package, ReleasePackage, download_release_by_pieces, \
    get_current_git_commit, get_devops_home, is_linux, is_mac
from ybops.utils import RELEASE_VERSION_PATTERN_WITH_BUILD
from ybops.utils.release import process_packages


def is_release_current(current_release_version, threshold_release_version):
    """
    Check if the curent release that we are testing is at least at the given threshold release.
    Returns True or False based on the check. Raises an exception if either releases has an
    invalid format.
    """

    logging.info("Current Version: {}, "
                 "Threshold Version: {}".format(current_release_version,
                                                threshold_release_version))
    c_match = re.match(RELEASE_VERSION_PATTERN_WITH_BUILD, current_release_version)
    t_match = re.match(RELEASE_VERSION_PATTERN_WITH_BUILD, threshold_release_version)
    if c_match is None or t_match is None:
        raise RuntimeError("Invalid input current={}, threshold={}".format(
            current_release_version, threshold_release_version))
    for i in range(1, 6):
        c = int(c_match.group(i) or '0')
        t = int(t_match.group(i) or '0')
        if c < t:
            # If any component is behind, the whole version is older.
            return False
        elif c > t:
            # If any component is ahead, the whole version is newer.
            return True
    # If all components were equal, then the versions are compatible.
    return True


def download_os_release(workdir, release_version, build_number):
    """
    Downloads the respective centos release from S3 release bucket.
    And do a local install.
    Returns location of package directory and local install directory.
    """

    if not workdir:
        release_dir = release_version + '-b' + str(build_number)
        workdir = os.path.join(BUILDS_DIR, release_dir)
    if not os.path.exists(workdir):
        os.makedirs(workdir)

    os_type = get_release_os("{}-b{}".format(release_version, str(build_number)))
    release_archive = "yugabyte-{}-b{}-{}-x86_64.tar.gz"\
        .format(release_version, str(build_number), os_type)
    release_archive_path = os.path.join(workdir, release_archive)

    if os.path.exists(release_archive_path):
        logging.info("Found previously downloaded: {}"
                     .format(release_archive_path))
    else:
        download_release_by_pieces(
            workdir,
            os_type=os_type,
            release_version=release_version,
            build_number=build_number,
            fail_on_error=True)
        if not os.path.exists(release_archive_path):
            raise RuntimeError("Expected package file not found: {}"
                               .format(release_archive_path))

    # Should not install centos package on macOS.
    if is_mac():
        return workdir, None

    install_dir = install_centos_release(
        workdir, release_archive_path, release_version)

    return workdir, install_dir


def install_centos_release(workdir, release_archive_path, release_version):
    install_dir = os.path.join(workdir, "yugabyte-" + release_version)
    if os.path.exists(install_dir):
        logging.info("Local install already exists: {}".format(install_dir))
        return install_dir

    command = ["tar", "-C", workdir, "-x", "-f", release_archive_path]
    logging.info("Local install: {}".format(command))
    try_command(command, num_retries=0)

    if not os.path.exists(install_dir):
        raise RuntimeError("Expected install directory not found: {}"
                           .format(install_dir))

    command = ["./bin/post_install.sh"]
    logging.info("Local install: {}".format(command))
    try_command(command, num_retries=0, cwd=install_dir)

    return install_dir


def release_packages(destination, custom_local_path=None, only_yugabyte_db=False):
    """
    Calls yb_release to generate all the relevant tar.gzs needed for a release.
    This follows the semantics of fetch_and_release_package:
    - If a custom path is specified, it will use the repo there, in whatever state it is in
    - If not specified, it will pull the latest code, on w/e branch is checked out

    Args:
        custom_local_path (str): custom base dir where to look for yugabyte-db subdir checkout
    """
    # TODO(bogdan): cleanup w/e BUILDS_DIR dependency we have here.
    return fetch_and_release_package(
        BUILDS_DIR,
        destination=destination,
        local_path=custom_local_path,
        # This will make jenkins not rebuild YB if it was already built.
        use_existing_releases=True,
        # Create package with all of the YB, YW, and devops packages.
        only_yugabyte_db=only_yugabyte_db)


class LocalReleaseContext:
    def __init__(self, local_path=None):
        # Inputs
        self.local_path = local_path
        if local_path:
            self.yugabyte_db_local = local_path
        else:
            self.yugabyte_db_local = None
        self.packages_location = None
        self.yugabyte_version = None
        self.build_number = None
        self.summary = ""
        self.should_upload = False
        self.docker_upload = is_linux()
        # Outputs
        self.has_errors = False
        self.repo_hashes = {}
        self.yb_release = None
        self.junit_test_suites = []

        # Test framework metadata.
        self.using_fixed_release = False
        self.tests_started = False
        self.email_subject = ""
        self.email_body = ""
        self.test_log = ""
        # Test record ID from release service
        self.test_id = None

    def update_commit_hashes(self, yb_hash):
        self.repo_hashes["itest"] = \
            get_current_git_commit(os.path.dirname(os.path.realpath(__file__)))
        devops_path = get_devops_home()
        self.repo_hashes["devops"] = get_current_git_commit(git_repo=devops_path)
        self.repo_hashes["yugabyte"] = yb_hash
        charts_path = os.path.join(devops_path, "..", "charts")
        if os.path.exists(charts_path):
            self.repo_hashes["charts"] = get_current_git_commit(git_repo=charts_path)

    def set_yb_release(self, build_number):
        self.yb_release = ReleasePackage.from_pieces(
            "yugabyte", self.yugabyte_version, None, build_type=None)
        self.yb_release.build_number = build_number

    def get_yb_version(self):
        if self.yb_release:
            return self.yb_release.get_release_name()
        else:
            return "{}-b{}".format(self.yugabyte_version, self.build_number)


def publish_java_client_jars(root_build_dir=None):
    # If nothing is passed in, assume BUILDS_DIR. This way instead of default value allows us to
    # pass in args.local_path, which could be None and then we still want to use BUILDS_DIR.
    # Note, the definition of local_path has now changed to be the YB directory itself, since all
    # 3 repos were merged into 1.
    root_build_dir = root_build_dir or os.path.join(BUILDS_DIR, "yugabyte-db")
    os.chdir(os.path.join(root_build_dir, "java"))
    publish_cmd = ["/usr/local/bin/mvn", "clean", "deploy", "-DskipTests"]
    output = ""
    try:
        output = subprocess.check_output(publish_cmd)
    except Exception as error:
        msg = "Failed deploying java client jars."
        logging.error("{}. Output: {}. Error: {}".format(msg, output, str(error)))
        raise RuntimeError(msg)


def publish_java_jars(context):
    if not context.should_upload:
        return
    try:
        logging.info("Publishing Java jars.")
        publish_java_client_jars(context.local_path)
        context.summary += "\tJava jar upload succeeded.\n"
    except (Exception, subprocess.CalledProcessError) as error:
        logging.error("Failed to deploy java jars through mvn. Error: {}".format(error))
        context.has_errors = True
        context.summary += "\t>>> {} <<< deploying java jars.\n".format(ITEST_FAILED)


def release_through_k8s(context):
    tmp_release_dir = tempfile.mkdtemp()
    logging.info("Dumping releases to {}".format(tmp_release_dir))
    atexit.register(lambda: shutil.rmtree(tmp_release_dir))

    # Only build yugaware packages on linux x86_64
    if is_linux() and platform.machine() == 'x86_64':
        logging.info("Preparing full yugabyte release")
        only_yugabyte_db = False
    else:
        logging.info("Preparing yugabyte-db only release")
        only_yugabyte_db = True

    # Release package pieces.
    release_packages(tmp_release_dir, context.local_path, only_yugabyte_db)
    # Repackage the build_number.
    repackage_release(tmp_release_dir, context, only_yugabyte_db)
    # Release the YW and YB docker images to the itest repos.
    if context.has_errors:
        logging.error("Skipping image creation due to earlier errors.")
        return
    if context.docker_upload:
        kubernetes.release_internal_repo(
            context.yb_release, "yugaware", context.packages_location)
        kubernetes.release_internal_repo(
            context.yb_release, "yugabyte", context.packages_location)
        # Do not build UBI images for < 2.6
        if is_release_current(context.yb_release.get_release_name(), "2.6.0.0-b0"):
            kubernetes.release_internal_repo(
                context.yb_release, "yugaware", context.packages_location, is_ubi=True)
            kubernetes.release_internal_repo(
                context.yb_release, "yugabyte", context.packages_location, is_ubi=True)
        # Also release the sample apps image, to be used by ptest.
        kubernetes.release_internal_repo(
            context.yb_release, "yb-sample-apps", context.packages_location,
            flag_latest=False)
    else:
        logging.info("Skipping docker images creation.")


def repackage_release(current_release_dir, context, only_yugabyte_db=False):
    """
    This is responsible for renaming releases from commit based to build_number based, as well as
    updating the YB release version JSON file to reflect the build number.

    This will also upload the final releases to the S3 release bucket, if running on the scheduler
    machine.

    Args:
        current_release_dir (str): Input directory where to find the tar.gz releases
        context (LocalReleaseContext): All the current in-memory release metadata
    """
    try:
        # Repackage the build number, retain the repo hashes, create yugabundle, and
        # upload if necessary. Also create yba_installer_full.
        # Only build this for releases >= 2.17.2.0 OR for
        # those >= 2.17.1.0-b100 (but not YBM releases that are >= 2.17.1.0-b1000)
        create_yba_installer_full = (not only_yugabyte_db and
            (is_release_current(context.yb_release.get_release_name(), "2.17.2.0-b0") or
                (is_release_current(context.yb_release.get_release_name(), "2.17.1.0-b100") and
                not is_release_current(context.yb_release.get_release_name(), "2.17.1.0-b1000"))))
        yb_hash = process_packages(
            current_release_dir, context.build_number,
            should_upload=context.should_upload, create_yugabundle=not only_yugabyte_db,
            create_yba_installer_full=create_yba_installer_full)['yugabyte']
        # Update the commit hashes to include the new YB hash.
        context.update_commit_hashes(yb_hash)
        if context.should_upload:
            context.summary += "\tSucessfully uploaded packages.\n"
        else:
            context.summary += "\tPrepared packages but asked not to upload them!\n"
        # Get all the files in the new temp directory and move them to the packages_location.
        for f in glob.glob("{}/*".format(current_release_dir)):
            logging.info("Moving {} to {}".format(f, context.packages_location))
            shutil.move(f, context.packages_location)
    except subprocess.CalledProcessError as error:
        context.has_errors = True
        context.summary += "Processing and uploading packages failed."
