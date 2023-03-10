import platform
import distro
import re
from utils import RELEASE_PACKAGE_TYPES


class ReleasePackage(object):
    def __init__(self):
        self.repo = None
        self.version = None
        self.commit = None
        self.build_number = None
        self.build_type = None
        self.system = None
        self.machine = None
        self.compiler = None

    @classmethod
    def from_pieces(cls, repo, version, commit, build_type=None):
        obj = cls()
        obj.repo = repo
        obj.version = version
        obj.commit = commit
        obj.build_type = build_type
        obj.system = platform.system().lower()
        if obj.system == "linux":
            obj.system = distro.linux_distribution(full_distribution_name=False)[0].lower()
        if len(obj.system) == 0:
            raise RuntimeError("Cannot release on this system type: " + platform.system())
        obj.machine = platform.machine().lower()

        obj.validate()
        return obj

    @classmethod
    def from_centos_release_pieces(cls, repo, version, build_number):
        """ Creates package info in the format of our centos release packages. """
        obj = cls()
        obj.repo = repo
        obj.version = version
        obj.build_number = build_number
        obj.system = "centos"
        obj.machine = "x86_64"
        return obj

    @classmethod
    def from_package_name(cls, package_name, is_official_release=False):
        obj = cls()
        obj.extract_components_from_package_name(package_name, is_official_release)

        obj.validate()
        return obj

    def extract_components_from_package_name(self, package_name, is_official_release):
        """
        There are two possible formats for our package names:
        - RC format, containing git hash and build type
          eg: <repo>[-ee]-<A.B.C.D>-<commit>[-<build_type>]-<system>-<machine>.tar.gz
        - Release format (is always release, so no need for build_type):
          eg: <repo>[-ee]-<A.B.C.D>-b<build_number>-<system>-<machine>.tar.gz

        Note that each of these types has an optional -ee for backwards compatibility to our
        previous enterprise vs community split. Also the yugabyte package has an optional build
        type, ie: -release, -debug, etc.
        """
        # Expect <repo>-<version>.
        pattern = "^(?P<repo>[^-]+)(?:-[^-]+)?-(?P<version>{})".format(RELEASE_VERSION_PATTERN)
        # If this is an official release, we expect a commit hash and maybe a build_type, else we
        # expect a "-b" and a build number.
        if is_official_release:
            # Add build number.
            pattern += "-b(?P<build_number>[0-9]+)"
        else:
            # Add commit hash and maybe build type.
            pattern += "-(?P<commit_hash>[^-]+)(-(?P<build_type>[^-]+))?"
        pattern += "(-(?P<compiler>[^-]+))?-(?P<system>[^-]+)-(?P<machine>[^-]+)\.tar\.gz$"
        match = re.match(pattern, package_name)
        if not match:
            raise RuntimeError("Invalid package name format: {}".format(package_name))
        self.repo = match.group("repo")
        self.version = match.group("version")
        self.build_number = match.group("build_number") if is_official_release else None
        self.commit = match.group("commit_hash") if not is_official_release else None
        self.build_type = match.group("build_type") if not is_official_release else None
        self.compiler = match.group("compiler")
        self.system = match.group("system")
        self.machine = match.group("machine")

    def validate(self):
        if self.repo not in RELEASE_PACKAGE_TYPES:
            raise RuntimeError("Invalid repo {}".format(self.repo))

    def get_release_package_name(self):
        return "{repo}-{release_name}-{system}-{machine}.tar.gz".format(
            repo=self.repo,
            release_name=self.get_release_name(),
            system=self.system,
            machine=self.machine)

    def get_release_name(self):
        # If we have a build number set, prioritize that to get the release version name, rather
        # than the internal commit hash name.
        release_name = self.version
        if self.build_number is not None:
            release_name += "-b{}".format(self.build_number)
        else:
            release_name += "-{}".format(self.commit)
            if self.build_type is not None:
                release_name += "-{}".format(self.build_type)
        return release_name
