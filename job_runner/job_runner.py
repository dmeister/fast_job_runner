""" Fast Job Runner """
import os
import multiprocessing
import logging
import random
import json
from time import time
import sys
import optparse
import signal
import job_runner_directory
import job_runner_util
import job_runner_configuration

class JobRunner:
    """ JobRunner class """
    def __init__(self, handler, directory, options):
        """ Constructor
            directory: directory configuration files are placed and searched
            handler: callback
            prefix: 
            options: options instance
            cores: Number of parallel jobs
        """
        self.cores = options.core_count
        self.directory = directory
        self.handler = handler
        self.options = options
 
    def run_job(self, options, filename, store_result=True):
        """ Runs a given job file """
        config = json.load(open(filename))
        return self.process_job(options, config, store_result)
    
    def call_handler(self, config):
        """  Calls the handler callback,
            Returns a tuple of the result dict, the runtime in seconds.
            An None result dict indicates an error, in this case, runtime will be None
        """
        try:
            typed_configuration = job_runner_configuration.ConfigurationType(config)

            # If the handler supports prepare, call it first
            if "prepare" in dir(self.handler):
                self.handler.prepare(self.options, typed_configuration)

            start_time = time()
            if "execute" in dir(self.handler):
                result = self.handler.execute(self.options, typed_configuration)
            elif "__call__" in dir(self.handler):
                result = self.handler(self.options, typed_configuration)
            else:
                raise Exception("Uncallable handler")
            end_time = time()

            if result is not None:
                result["runtime"] = (end_time - start_time)

            return result
        except Exception as e:
            logger.exception("")
            # An execution during the execution is counted as an error
            return None
        finally:
            # If the handler supports cleanup, call it
            if "cleanup" in dir(self.handler):
                self.handler.cleanup(self.options, typed_configuration)

    def process_job(self, options, config, store_result=True):
        """ Runs a given job configuration """

        def get_result_filename():
            """ Returns the result filename """
            return os.path.abspath(
                self.directory.join("results", 
                self.directory.config_filename(config)))

        if store_result and not self.directory:
            raise Exception("Cannot store result without directory")

        logger.info('Run %s', job_runner_util.pretty_dict(config))

        if store_result:
            # Check if result file is already existing, return old result in this case
            # Do not re-run a configuration
            if os.path.exists(get_result_filename()):
                return json.loads(open(get_result_filename()).read())

        result= self.call_handler(config)
        if result:
            # A return value indicates a successful run
            logger.info("Run %s finished: %s", config, result)
            result["config"] = config
                
            if store_result:
                json.dump(result, open(get_result_filename(), "w"), 
                    sort_keys=True, 
                    indent=4)
        else:
            logger.warn("Execution error: %s", config)
        return result

    def get_moved_job_filename(self, directory_key, job_filename):
        return self.directory.join(directory_key, os.path.basename(job_filename))

    def select_job_file(self):
        """ Selects a random job file """
        job_files = os.listdir(self.directory("queue"))
        while len(job_files) > 0:    
            job_file = self.directory.join("queue", random.choice(job_files))
            yield job_file

            # Update job list
            job_files = os.listdir(self.directory("queue"))

    def job_runner(self):
        """ Starts the job runner loop.
            Runs until no jobs are left to execute
        """
        logging.info("Job runner starts")

        if not os.path.exists(self.directory("queue")):
            raise Exception("Cannot start job runner without job directory")

        for job_file in self.select_job_file():
            try:    
                # Select a random job file
                job_wip_file = self.get_moved_job_filename("wip", job_file)
                
                logger.debug("Process config file: %s", job_file)
        
                # The process claims the job configuration by moving it to the work-in-progress directory
                try:
                    os.rename(job_file, job_wip_file)
                except OSError:
                    # Hmm, is the file gone?
                    if not os.path.exists(job_file) and os.path.exists(job_wip_file):
                        # Somebody else grapped the same file
                        # Just, fetch the next file
                        continue
                    else:
                        # Raise the error again
                        raise
                
                if self.run_job(self.options, job_wip_file):
                    job_finished_file = self.get_moved_job_filename("done", job_file)
                    os.rename(job_wip_file, job_finished_file)
                else:
                    # Execution error
                    job_error_file = self.get_moved_job_filename("error", job_file)
                    os.rename(job_wip_file, job_error_file)
            except (KeyboardInterrupt, SystemExit, Exception):
                job_failure_file = self.get_moved_job_filename("failure", job_file)
                logger.exception("Execution failure: %s", job_file)
                os.rename(job_wip_file, job_failure_file)
                raise
        logger.info("Job runner stops")

    def start(self):
        """ Start the job execution and (potentially) multiple cores on the same node
        """
        if not self.directory():
            raise Exception("Job runner not startable without directory")
            
        # Start job runner in subprocesses
        processes = [multiprocessing.Process(target=self.job_runner) for _ in xrange(self.cores)]
        map(lambda p: p.start(), processes)
        map(lambda p: p.join(), processes)

def create_job_runner(handler, prefix, parser = None, full_argv = sys.argv):
    def configure_logging(options):
        format = "%(asctime)-15s %(message)s"
        if options.debug:
            level=logging.DEBUG
        elif options.silent:
            level=logging.WARN
        else:
            level=logging.INFO
        logging.basicConfig(level=level, 
                format=format, 
                stream=sys.stdout)

    def configure_parser(parser):
        if parser is None:
            # Optparse should not be used for new code, but this has to run on 
            # heavily outdated clusters
            usage = """usage: %prog [options] arg1 arg2

%prog -g      - Generate configuration files in directory
%prog         - Run generated configuration files until no file is left
%prog <file1> - Run configuration of <file1>
"""
            parser = optparse.OptionParser(usage=usage)
        # Add these options even if existing parser is used
        parser.add_option("--debug",
            dest="debug",
            help="Adds debug output to console",
            action="store_true")
        parser.add_option("--silent",
            dest="silent",
            help="Only display warning and error messages",
            action="store_true")
        parser.add_option("-d", "--directory", 
            dest="directory", 
            help="Base directory to use by the job runner (default=\".\")",
            default=".")
        parser.add_option("-g", 
            "--generate",
            dest="generate_configs",
            help="Generate configuration files",
            action="store_true")
        parser.add_option("--cores", 
            default=1, 
            dest="core_count", 
            help="Number of parallel job executions on current machine",
            type=int)
        (options, argv) = parser.parse_args(full_argv)
        argv = argv[1:]
        if options.debug and options.silent:
            parser.error("--debug and --silent are mutually exclusive")
        if options.generate_configs and len(argv) > 0:
            parser.error("--generate and arguments are mutually exclusive")
        if options.core_count < 0:
            parser.error("The core count should be positive")
        return (options, argv)

    (options, argv) = configure_parser(parser)
    configure_logging(options)

    jr_directory = job_runner_directory.JobRunnerDirectory(options.directory, prefix)
    jr = JobRunner(handler, 
            jr_directory, 
            options = options)
    return (jr, argv)

def main(handler, prefix = None, configuration_space = None, parser = None, install_signal=True):
    if install_signal:
        # Allows to act on a SIGTERM signal to move the current configuration file
        # away from the wip directory
        def sys_exit_handler(signum, frame):
            sys.exit(1)
        signal.signal(signal.SIGTERM, sys_exit_handler)

    (jr, argv) = create_job_runner(handler, prefix, parser)

    if configuration_space and jr.options.generate_configs:
        # Fix type
        if type(configuration_space) == dict:
            configuration_space = job_runner_configuration.ConfigurationSpace(configuration_space)
        configuration_space.generate_files(jr.directory)  
    elif len(argv) > 0:
        for arg in argv:
            o = jr.run_job(options, arg)
            logging.info(job_runner_util.pretty_dict(o))
    else:
        jr.start()
    