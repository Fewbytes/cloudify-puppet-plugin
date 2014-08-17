# Based on
# https://github.com/CloudifySource/cloudify-recipes/blob/
# 991ab4ce0596930836f7d4e33f6f9bd70894d85a/
# services/puppet/PuppetBootstrap.groovy
import datetime
import json
import os
import platform
import re
import requests
import subprocess
import tempfile
import urlparse

from cloudify.exceptions import NonRecoverableError

from retrying_lock import RetryingLock

# (lock_name, retries, sleep)
PUPPET_INSTALL_LOCK = ('puppet-install.lock', 30, 10)
PUPPET_CONFIG_LOCK = ('puppet-config.lock', 30, 10)

PUPPET_CONF_TPL = """# This file was generated by Cloudify
[main]
    ssldir = /var/lib/puppet/ssl
    environment = {environment}
    pluginsync = true
    logdir = /var/log/puppet
    vardir = /var/lib/puppet
    classfile = $vardir/classes.txt
    factpath = /opt/cloudify/puppet/facts:$vardir/lib/facter:$vardir/facts
    modulepath = {modulepath}

[agent]
    server = {server}
    certname = {certname}
    node_name_value = {node_name}
"""

PUPPET_CONF_MODULE_PATH = [
    '/etc/puppet/modules',
    '/usr/share/puppet/modules',
    '/opt/cloudify/puppet/modules',
    # {cloudify_module_path}
]
# docs.puppetlabs.com/puppet/latest/reference/lang_reserved.html#tags
PUPPET_TAG_RE = re.compile('\A[a-z0-9_][a-z0-9_:\.\-]*\Z')
# docs.puppetlabs.com/puppet/latest/reference/lang_reserved.html#environments
PUPPET_ENV_RE = re.compile('\A[a-z0-9]+\Z')


def quote_shell_arg(s):
    return "'" + s.replace("'", "'\"'\"'") + "'"


def is_resource_url(url):
    """
    Tells wether a URL is pointing to a resource (which is uploaded with
    the blueprint.
    '/xyz.tar.gz' URLs are pointing to resources.
    """
    u = urlparse.urlparse(url)
    return (not u.scheme), u.path


class PuppetError(RuntimeError):
    """An exception for all Puppet related errors"""


class SudoError(PuppetError):

    """An internal exception for failures when running
    an OS command with sudo"""


class PuppetInternalLogicError(PuppetError):
    pass


class PuppetParamsError(PuppetError):
    """ Invalid parameters were supplied """


def _context_to_struct(ctx):
    return {
        'node_id': ctx.node_id,
        'node_name': ctx.node_name,
        'blueprint_id': ctx.blueprint_id,
        'deployment_id': ctx.deployment_id,
        'properties': ctx.properties,
        'runtime_properties': ctx.runtime_properties,
        'capabilities': _try_extract_capabilities(ctx),
        'host_ip': _try_extract_host_ip(ctx)
    }


def _related_to_struct(related):
    return {
        'node_id': related.node_id,
        'properties': related.properties,
        'runtime_properties': related.runtime_properties,
        'host_ip': _try_extract_host_ip(related)
    }


def _try_extract_capabilities(ctx):
    try:
        return ctx.capabilities.get_all()
    except AttributeError:
        return {}


def _try_extract_host_ip(ctx_or_related):
    try:
        return ctx_or_related.host_ip
    except NonRecoverableError:
        return None


class PuppetManager(object):

    # Copy+paste from Chef plugin - start
    def _log_text(self, title, prefix, text):
        ctx = self.ctx
        if not text:
            return
        ctx.logger.info('*** ' + title + ' ***')
        for line in text.splitlines():
            ctx.logger.info(prefix + line)

    def _sudo(self, *args):
        """a helper to run a subprocess with sudo, raises SudoError"""

        ctx = self.ctx

        def get_file_contents(f):
            f.flush()
            f.seek(0)
            return f.read()

        cmd = ["/usr/bin/sudo"] + list(args)
        ctx.logger.info("Running: '%s'", ' '.join(cmd))

        # TODO: Should we put the stdout/stderr in the celery logger?
        #       should we also keep output of successful runs?
        #       per log level? Also see comment under run_chef()
        stdout = tempfile.TemporaryFile('rw+b')
        stderr = tempfile.TemporaryFile('rw+b')
        out = None
        err = None
        try:
            subprocess.check_call(cmd, stdout=stdout, stderr=stderr)
            out = get_file_contents(stdout)
            err = get_file_contents(stderr)
            self._log_text("stdout", "  [out] ", out)
            self._log_text("stderr", "  [err] ", err)
        except subprocess.CalledProcessError as exc:
            raise SudoError("{exc}\nSTDOUT:\n{stdout}\nSTDERR:{stderr}".format(
                exc=exc,
                stdout=get_file_contents(stdout),
                stderr=get_file_contents(stderr)))
        finally:
            stdout.close()
            stderr.close()

        return out, err

    def _sudo_write_file(self, filename, contents):
        """a helper to create a file with sudo"""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(contents)

        self._sudo("mv", temp_file.name, filename)

    def _prog_available_for_root(self, prog):
        with open(os.devnull, "w") as fnull:
            which_exitcode = subprocess.call(
                ["/usr/bin/sudo", "which", prog], stdout=fnull, stderr=fnull)
        return which_exitcode == 0

    # Copy+paste from Chef plugin - end

    # http://stackoverflow.com/a/5953974
    def __new__(cls, ctx):
        """ Transparent factory. PuppetManager() returns a class
        assembled from PuppetManager class the relevant subclasses of
        PuppetInstaller and PuppetRunner """
        if cls is PuppetManager:
            r = PuppetRunner.get_runner_class(ctx)
            i = PuppetInstaller.get_installer_class()
            cls = type(r.__name__ + i.__name__, (r, i, PuppetManager), {})
            ctx.logger.debug("PuppetManager class: {0}".format(cls))
        # Disable magic for subclasses
        return super(PuppetManager, cls).__new__(cls, ctx)

    def __init__(self, ctx):
        self.ctx = ctx
        self.props = self.ctx.properties['puppet_config']
        self.environment = None
        self.process_properties()

    def puppet_is_installed(self):
        return self._prog_available_for_root('puppet')

    def install(self):
        with RetryingLock(self.ctx, *PUPPET_INSTALL_LOCK):
            if self.puppet_is_installed():
                self.ctx.logger.info("Not installing Puppet as "
                                     "it's already installed")
                return
            url = self.get_repo_package_url()
            response = requests.head(url)
            if response.status_code != requests.codes.ok:
                raise PuppetError("Repo package is not available (at {0})".
                                  format(url))

            self.ctx.logger.info("Installing package from {0}".format(url))
            self.install_package_from_url(url)
            self.refresh_packages_cache()
            for p in 'puppet-common', 'puppet':
                self.install_package(p,
                                     self.props.get('version',
                                                    self.DEFAULT_VERSION))
            for package_name in self.EXTRA_PACKAGES:
                self.install_package(package_name)

            self._sudo("mkdir", "-p", *self.DIRS.values())
            self._sudo("chmod", "700", *self.DIRS.values())
            self.install_custom_facts()

    def refresh_packages_cache(self):
        pass

    def install_custom_facts(self):
        facts_source_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'puppet', 'facts', 'cloudify_facts.rb')
        facts_destination_path = self.DIRS['local_custom_facts']
        self.ctx.logger.info("Installing custom facts {0} to {1}".format(
            facts_source_path,
            facts_destination_path))
        self._sudo('cp', facts_source_path, facts_destination_path)


# *** Installer ***


class RubyGemJsonExtraPackageMixin(object):
    EXTRA_PACKAGES = ["rubygem-json"]


class PuppetInstaller(object):
    EXTRA_PACKAGES = []
    DEFAULT_VERSION = '3.5.1-1puppetlabs1'
    DIRS = {
        'local_repo': os.path.expanduser('~/cloudify/puppet'),
        'local_custom_facts': '/opt/cloudify/puppet/facts',
        'cloudify_module': '/opt/cloudify/puppet/modules/cloudify',
    }

    @classmethod
    def get_installer_class(cls):
        classes = cls.__subclasses__()
        classes = [c for c in classes if c._installer_handles()]
        if len(classes) != 1:
            raise PuppetInternalLogicError(
                "Failed to find correct PuppetInstaller")
        return classes[0]


class PuppetDebianInstaller(PuppetInstaller):

    @staticmethod
    def _installer_handles():
        return platform.linux_distribution()[0].lower() in (
            'debian', 'ubuntu', 'mint')

    def get_repo_package_url(self):
        ver = platform.linux_distribution()
        if ver[2]:
            ver = ver[2]
        else:
            if ver[1].endswith('/sid'):
                ver = 'sid'
            else:
                raise PuppetError("Fail to detect Linux distro version")

        url = self.props.get('repos', {}).get('deb', {}).get(ver)
        return (
            url
            or
            'http://apt.puppetlabs.com/puppetlabs-release-{0}.deb'.format(ver))

    def install_package_from_url(self, url):

        name = os.path.basename(urlparse.urlparse(url).path)

        pkg_file = tempfile.NamedTemporaryFile(suffix='.'+name, delete=False)
        self.ctx.logger.info("Using temp file {0} for package installation".
                             format(pkg_file.name))
        pkg_file.write(requests.get(url).content)
        pkg_file.flush()
        pkg_file.close()
        self._sudo('dpkg', '-i', pkg_file.name)
        os.remove(pkg_file.name)

    def refresh_packages_cache(self):
        self._sudo('apt-get', 'update')

    # XXX: package_version is not sanitized
    def install_package(self, package_name, package_version=None):
        if package_version is None:
            p = package_name
        else:
            p = package_name + '=' + str(package_version)
        self._sudo('apt-get', 'install', '-y', p)


class PuppetRHELInstaller(RubyGemJsonExtraPackageMixin, PuppetInstaller):
    """ UNTESTED """

    @staticmethod
    def _installer_handles():
        return platform.linux_distribution()[0] in (
            'redhat', 'centos', 'fedora')

    def get_repo_package_url(self):
        raise NotImplementedError()

    def install_package_from_url(self, url):
        self._sudo("rpm", "-ivh", url)

    # XXX: package_version is not sanitized
    def install_package(self, package_name, package_version=None):
        if package_version is None:
            p = package_name
        else:
            p = package_name + '-' + str(package_version)
        self._sudo('yum', 'install', '-y', p)

# *** Runner ***


class PuppetRunner(object):

    @staticmethod
    def get_runner_class(ctx):
        if 'server' in ctx.properties['puppet_config']:
            cls = PuppetAgentRunner
        else:
            cls = PuppetStandaloneRunner
        return cls

    def configure(self):
        pass

    def get_run_env_vars(self):
        return {}

    def set_environment(self, e):
        env = re.sub('[- .]', '_', e)
        if not PUPPET_ENV_RE.match(env):
            raise PuppetParamsError(
                "puppet_config.environment must contain only alphanumeric "
                "characters, you gave '{0}'".format(env))
        self.environment = env

    def run(self, tags=None, execute=None, manifest=None):
        ctx = self.ctx
        self.execute = execute
        self.manifest = manifest
        self.install()
        self.configure()
        facts = self.props.get('facts', {})
        if 'cloudify' in facts:
            raise PuppetError("Puppet attributes must not contain 'cloudify'")
        facts['cloudify'] = _context_to_struct(ctx)
        if ctx.related:
            facts['cloudify']['related'] = _related_to_struct(ctx.related)
        t = 'puppet.{0}.{1}.{2}.'.format(
            ctx.node_name, ctx.node_id, os.getpid())
        temp_file = tempfile.NamedTemporaryFile
        facts_file = temp_file(prefix=t, suffix=".facts_in.json", delete=False)
        json.dump(facts, facts_file, indent=4)
        facts_file.close()

        cmd = [
            "puppet",
        ] + self.get_runner_cmd() + [
            "--detailed-exitcodes",
            "--logdest", "console",
            "--logdest", "syslog"
        ]

        if tags:
            cmd += ['--tags', ','.join(tags)]

        cmd = ' '.join(cmd)

        environ = self.get_run_env_vars()
        environ = ["export {0}='{1}'\n".format(k, v)
                   for k, v in environ.items()]
        environ = ''.join(environ)
        run_file = temp_file(prefix=t, suffix=".run.sh", delete=False)
        run_file.write(
            '#!/bin/bash -e\n'
            'export FACTERLIB={0}\n'
            'export CLOUDIFY_FACTS_FILE={1}\n{2}'
            'e=0\n'
            .format(self.DIRS['local_custom_facts'], facts_file.name, environ)
            + cmd + ' || e=$?\n'
            'echo Exit code: $e\n'
            'if [ $e -eq 1 ];then exit 1;fi\n'
            'if [ $(($e & 4)) -eq 4 ];then exit 4;fi\n'
            'exit 0\n'
        )
        run_file.close()
        self._sudo('chmod', '+x', run_file.name)
        self.ctx.logger.info("Will run: '{0}' (in {1})".format(cmd,
                                                               run_file.name))
        self._sudo(run_file.name)

        os.remove(facts_file.name)

    def get_modules_path(self):
        local_modules_path = os.path.join(self.DIRS['local_repo'], 'modules')
        modulepath = ':'.join(PUPPET_CONF_MODULE_PATH + [local_modules_path])
        return modulepath


class PuppetAgentRunner(PuppetRunner):

    def process_properties(self):
        p = self.props
        if 'environment' not in p:
            raise PuppetParamsError("puppet_config.environment is missing")
        self.set_environment(p['environment'])

    def get_runner_cmd(self):
        return ["agent", "--onetime", "--no-daemonize"]

    def _get_config_file_contents(self):
        p = self.props
        node_name = (
            p.get('node_name_prefix', '') +
            self.ctx.node_id +
            p.get('node_name_suffix', '')
        )
        certname = (
            datetime.datetime.utcnow().strftime('%Y%m%d%H%M') +
            '-' +
            node_name
        )

        conf = PUPPET_CONF_TPL.format(
            environment=self.environment,
            modulepath=self.get_modules_path(),
            server=self.props['server'],
            certname=certname,
            node_name=node_name,
        )
        return conf

    def configure(self):
        contents = self._get_config_file_contents()
        self._sudo_write_file('/etc/puppet/puppet.conf', contents)


class PuppetStandaloneRunner(PuppetRunner):
    def process_properties(self):
        props = self.props
        if 'environment' in props:
            self.set_environment(props['environment'])
        if 'modules' in props:
            if not isinstance(props['modules'], list):
                raise RuntimeError("puppet_config.modules must be a list")
        if ('execute' not in props) and ('manifest' not in props):
            raise PuppetParamsError("Either 'execute' or 'manifest' "
                                    "must be specified under 'puppet_config'."
                                    "None are specified.")

    def get_run_env_vars(self):
        return {'FACTER_CLOUDIFY_LOCAL_REPO': self.DIRS['local_repo']}

    def get_installed_modules(self):
        ret = set()
        # Ugly output parsing :(
        out, _ = self._sudo('puppet', 'module', 'list', '--modulepath',
                            self.get_modules_path())
        out = out.split()
        prev = None
        for cur in out:
            if cur.startswith('('):
                ret.add(prev)
            prev = cur
        return ret

    def configure(self):
        with RetryingLock(self.ctx, *PUPPET_CONFIG_LOCK):
            props = self.props
            modules = props.get('modules', [])
            for module in modules:
                installed_modules = self.get_installed_modules()
                if module not in installed_modules:
                    self._sudo('puppet', 'module', 'install', module)
            # Download after modules allows overriding
            if 'download' in props:
                download = props['download']
                if not isinstance(download, list):
                    download = [download]
                for dl in download:
                    self._url_to_dir(dl, self.DIRS['local_repo'])

    def get_runner_cmd(self):
        cmd = [
            "apply",
            "--modulepath={0}".format(self.get_modules_path()),
        ]

        if self.environment:
            cmd += ['--environment', self.environment]

        cmd_done = False
        e = self.execute
        if e:
            cmd += ["--execute", quote_shell_arg(e)]
            cmd_done = True

        m = self.manifest
        if m:
            cmd += [quote_shell_arg(os.path.join(self.DIRS['local_repo'], m))]
            cmd_done = True

        if not cmd_done:
            raise PuppetParamsError("Either 'execute' or 'manifest' " +
                                    "must be specified. None are specified")

        return cmd

    def _url_to_dir(self, url, dst_dir):
        """
        Downloads .tar.gz from `url` and extracts to `dst_dir`.
        If URL is relative ("/xyz.tar.gz"), it's fetched using
        download_resource().
        """

        if url is None:
            return

        ctx = self.ctx

        ctx.logger.info(
            "Downloading from {0} and unpacking to {1}".format(url, dst_dir))
        temp_archive = tempfile.NamedTemporaryFile(
            suffix='.url_to_dir.tar.gz', delete=False)

        is_resource, path = is_resource_url(url)
        if is_resource:
            ctx.logger.info("Getting resource {0} to {1}".format(path,
                            temp_archive.name))
            ctx.download_resource(path, temp_archive.name)
        else:
            ctx.logger.info("Downloading from {0} to {1}".format(url,
                            temp_archive.name))
            temp_archive.write(requests.get(url).content)
            temp_archive.flush()
            temp_archive.close()

        command_list = [
            'sudo',
            'tar', '-C', dst_dir,
            '--xform', 's#^' + os.path.basename(dst_dir) + '/##',
            '-xzf', temp_archive.name]
        try:
            ctx.logger.info("Running: '%s'", ' '.join(command_list))
            subprocess.check_call(command_list)
        except subprocess.CalledProcessError as exc:
            raise PuppetError("Failed to extract file {0} to directory {1} "
                              "which was downloaded from {2}. Command: {3}. "
                              "Exception: {4}".format(
                                  temp_archive.name,
                                  dst_dir,
                                  url,
                                  command_list,
                                  exc))

        os.remove(temp_archive.name)  # on failure, leave for debugging
        # try:
        #     os.rmdir(os.path.join(dst_dir, os.path.basename(dst_dir)))
        # except OSError as e:
        #     if e.errno != errno.ENOENT:
        #         raise e
