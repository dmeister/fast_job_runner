#!/usr/bin/python
import job_runner
import subprocess

def execute(options, config):
	output = subprocess.check_output([config.command, config.dir])
	return {
		"line count": len(output.split()),
		"length": len(output)
	}

if __name__ == "__main__":
	configuration_space = {
		"command": ["ls", "df"],
		"dir": ["/", "/root", "/etc"]
	}
	job_runner.main(execute, "sample", configuration_space)