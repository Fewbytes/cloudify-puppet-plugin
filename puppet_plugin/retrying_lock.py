import os

import lockfile

__all__	= ['RetryingLock']


class RetryingLock(object):

    def __init__(self, ctx, lock_name, retries, sleep):

        tmp_dir = os.environ.get('TMPDIR', '/tmp')
        path = os.path.join(tmp_dir, lock_name)

        self.ctx = ctx
        self.path = path
        self.retries = retries
        self.sleep = sleep
        self.acquired = False

    def __enter__(self):
        self.ctx.logger.info("Using lock file {0}".format(self.path))
        self.file = lockfile.LockFile(self.path)
        for i in range(0, self.retries):
            try:
                self.file.acquire(timeout=self.sleep)
            except lockfile.LockTimeout:
                self.ctx.logger.info("Could not lock the file '{0}'. "
                                     "Will sleep for {1} seconds and then try "
                                     "again.".format(self.path, self.sleep))
            else:
                self.acquired = True
                self.ctx.logger.info("Acquired lock the file '{0}'."
                                     .format(self.path))
                return
        raise RuntimeError("Failed to lock the file '{0}'.".format(self.path))

    def __exit__(self, _exc_type, _v, _tb):
        if not self.acquired:
            return
        self.file.release()
        self.acquired = False
        self.ctx.logger.info("Released lock the file '{0}'.".format(self.path))
