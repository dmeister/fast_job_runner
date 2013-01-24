import copy
import os
import job_runner_directory
import logging
import json

logger = logging.getLogger("job runner")

class ConfigurationType:
     def __init__(self, dictionary):
         for k, v in dictionary.items():
            if type(v) == dict:
                v = ConfigurationType(v)
            setattr(self, k, v)

class ConfigurationSpace:
    def __init__(self, configuration_dict, filter_list= None, start = 0, count = 1):
        self.configuration_dict = configuration_dict

        self.filter_list = filter_list
        if not self.filter_list:
            self.filter_list = []

        self.start = start
        self.count = count

    def add_filter(self, filter_callback):
        """ Adds a new filter check
            Only configurations that pass all filters are created
        """
        self.filter_list.append(filter_callback)
        return self

    def expand_configs(self, config):
        """ Recursivly expands the configuration dict """
        config = dict(config)
        for (key, value) in config.items():
            if type(value) == list:
                # Explore all list elements
                expanded_list = []
                for value_item in value:
                    sub_config = copy.deepcopy(config)
                    sub_config[key] = copy.deepcopy(value_item)
                    expanded_list.extend(
                            self.expand_configs(sub_config)
                    )
                return expanded_list
            elif type(value) == dict:
                # Recursivly expand all dict elements
                expanded_values = self.expand_configs(copy.deepcopy(value))
                el = []
                for expanded_value in expanded_values:
                    e = copy.deepcopy(config)
                    del e[key]
                    for e in self.expand_configs(e):
                        e[key] = expanded_value
                        el.append(e)
                return el

        return [dict(config)]

    def check_filter_list(self, configuration):
        c = ConfigurationType(configuration)
        return all(filter_handler(c) for filter_handler in self.filter_list)
        
    def generate_files(self, jr_directory):
        """ Generated an expanded set of configuration files from a configuration dict """
        if not jr_directory():
            raise Exception("Job runner cannot generate configurations without directory")
        
        logger.debug("Start generating configuration space")

        # For a given number of runs
        for run in xrange(self.start, self.start + self.count):
            c = {
                "run": run
            }
            c.update(self.configuration_dict)

            # Expand configuration
            for specific_config in self.expand_configs(c):
                if not self.check_filter_list(specific_config):
                    continue
                logger.info(specific_config)
                self.write_config_file(jr_directory, specific_config)

    def write_config_file(self, jr_directory, config):
        """ Writes a given configuration if the configuration didn't existed before """

        def check_if_exists(filename):
            """ Checks if the configuration file already exists in some job directory """
            possible_files = [os.path.join(d, filename) for d in jr_directory.all()]
            return any(os.path.exists(f) for f in possible_files)

        if not "run" in config:
            config["run"] = 1
            
        filename = jr_directory.config_filename(config)
        if check_if_exists(filename):
            logger.debug("Skip job file %s", filename)
            return False
        
        logger.debug("Write job file %s", filename)
        job_config_file = open(jr_directory.join("queue", filename), "w")
        json.dump(config, job_config_file, indent=4, sort_keys=True)
        return True