import job_runner_util
import os
import logging

logger = logging.getLogger("job runner")

class JobRunnerDirectory:
    def __init__(self, base_directory, prefix):
        def get_or_make_dir(d):
            """ Return sub directory
                Creats the directory first if necessary
            """
            d = os.path.join(self.base_directory, d)
            if not os.path.exists(d):
                logger.debug("Create directory %s", d)
                os.mkdir(d)
            return d

        self.base_directory = base_directory
        self.prefix = prefix

        if base_directory and prefix:
            self.directory_map = {}
            for key in ["results", 
                    "queue", 
                    "wip", 
                    "done", 
                    "error", 
                    "failure"]:
                if prefix:
                    dir_name = "%s-%s" % (prefix, key)
                else:
                    dir_name = key
                self.directory_map[key] = get_or_make_dir(dir_name)

        # Ensure that all directories lie on same file system. This is needed for move atomicity
        # Without openat calls, there is still a race condition that somebody is so smart to
        # over-mount the directories, but I don't care
        if len(set([os.stat(d).st_dev for d in self.directory_map.values()])) > 1:
            raise Exception("Illegal directory setting: Directories are not on same file system")

    def __call__(self, key = None):
        if key is None:
            return self.base_directory
        return self.directory_map[key]

    def all(self):
        return self.directory_map.values()

    def join(self, directory_key, filename):
        return os.path.join(self(directory_key), filename)

    def config_filename(self, config):
        """ Returns the configuration filename of a configuration dict """
        return "%s.cfg" % job_runner_util.hash_config(config)