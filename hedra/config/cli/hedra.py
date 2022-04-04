from hedra.testing import ActionSet


hedra_cli = {
    "name": "Hedra",
    "description": "Python load testing tool offering asynch, parallel, and distributed execution.",
    "arguments": [
        {
            "map_name": "log_level",
            "data_type": "string",
            "arg_name": "--log-level",
            "var_name": "log_level",
            "arg_type": "value",
            "default": "info",
            "required": False,
            "help": "Sets log level verbosity. Options are - error, warning, info, and debug."
        },
        {
            "map_name": "actions",
            "data_type": "dict",
            "arg_name": "--actions-filepath",
            "var_name": "actions_filepath",
            "arg_type": "file",
            "default": "./actions.json",
            "envar_default": "ACTIONS_FILEPATH",
            "required": False,
            "help": "The path to the actions JSON file for Hedra to load."
        },
        {
            "map_name": "code_actions",
            "data_type": "list",
            "arg_name": "--code-filepath",
            "var_name": "code_filepath",
            "arg_type": "python-file",
            'class_type': ActionSet,
            "envar_default": "CODE_FILEPATH",
            "required": False,
            "help": "The path to the actions Python file for Hedra to load."
        },
        {
            "map_name": "executor_config",
            "data_type": "string",
            "arg_name": "--batch-interval-range",
            "var_name": "batch_interval_range",
            "data_key": "batch_interval_range",
            "arg_type": "value",
            "envar_default": "BATCH_INTERVAL_RANGE",
            "required": False,
            "help": "A pair of integers specified as - 'min:max' - to use for random batch interval wait range."
        },
        {
            "map_name": "config",
            "data_type": "dict",
            "arg_name": "--config-filepath",
            "var_name": "config_filepath",
            "arg_type": "file",
            "envar_default": "CONFIG_FILEPATH",
            "required": False,
            "help": "The path to an optional config JSON file containing Hedra CLI arg equivalents."
        },
        {
            "map_name": "runner_mode",
            "data_type": "string",
            "data_key": "runner_mode",
            "arg_name": "--runner-mode",
            "var_name": "runner_mode",
            "arg_type": "value",
            "default": "local",
            "envar_default": "HEDRA_EXECUTION_MODE",
            "required": False,
            "help": "Determines which of Hedra's runners is used. Ex. - local, leader, worker, etc."
        },
        {
            "map_name": "executor_config",
            "data_type": "string",
            "data_key": "engine_type",
            "arg_name": "--engine",
            "var_name": "engine",
            "arg_type": "value",
            "envar_default": "HEDRA_ENGINE",
            "default": "http",
            "required": False,
            "help": "Determines the engine Hedra uses to execute actions (i.e. the type of action)."
        },
        {
            "map_name": "executor_config",
            "data_type": "string",
            "data_key": "session_url",
            "arg_name": "--session-url",
            "var_name": "session_url",
            "arg_type": "value",
            "envar_default": "SESSION_URL",
            "required": False,
            "help": "URL used by the GraphQL engine for the duration of the session."
        },
        {
            "map_name": "executor_config",
            "data_type": "string",
            "data_key": "persona_type",
            "arg_name": "--persona",
            "var_name": "persona",
            "arg_type": "value",
            "envar_default": "HEDRA_PERSONA",
            "default": "simple",
            "required": False,
            "help": "Determines the persona Hedra uses to schedule, group, and organize action execution."
        },
        {
            "map_name": "executor_config",
            "data_type": "string",
            "data_key": "total_time",
            "arg_name": "--total-time",
            "var_name": "total_time",
            "arg_type": "value",
            "required": False,
            "help": "A string of format HH:MM:SS or a integer of total seconds for which Hedra should run."
        },
        {
            "map_name": "executor_config",
            "data_type": "integer",
            "data_key": "batch_interval",
            "arg_name": "--batch-interval",
            "var_name": "batch_interval",
            "default": 1,
            "arg_type": "value",
            "required": False,
            "help": "How long Hedra should pause (in seconds) between execution of batches of actions."
        },
        {
            "map_name": "executor_config",
            "data_type": "integer",
            "data_key": "batch_size",
            "arg_name": "--batch-size",
            "var_name": "batch_size",
            "arg_type": "value",
            "required": False,
            "help": "The size of each batch (a grouping) of actions Hedra should execute at once."
        },
        {
            "map_name": "executor_config",
            "data_type": "integer",
            "data_key": "batch_count",
            "arg_name": "--batch-count",
            "var_name": "batch_count",
            "arg_type": "value",
            "required": False,
            "help": "The number of batches (groupings) of actions Hedra should execute in total."
        },
        {
            "map_name": "executor_config",
            "data_type": "float",
            "data_key": "batch_time",
            "arg_name": "--batch-time",
            "var_name": "batch_time",
            "arg_type": "value",
            "required": False,
            "help": "The maximum amount of time (in seconds) a batch (a grouping) of actions should execute."
        },
        {
            "map_name": "executor_config",
            "data_type": "float",
            "data_key": "gradient",
            "arg_name": "--gradient",
            "var_name": "gradient",
            "arg_type": "value",
            "required": False,
            "help": "The decimal percentage rate at which the ramped parameter of a persona will increase."
        },
        {
            "map_name": "executor_config",
            "data_type": "float",
            "data_key": "request_timeout",
            "arg_name": "--request-timeout",
            "var_name": "request_timeout",
            "arg_type": "value",
            "required": False,
            "help": "The (decimal) amount of time each request should wait before timing out."
        },
        {
            "map_name": "executor_config",
            "data_type": "integer",
            "data_key": "warmup",
            "arg_name": "--warmup",
            "var_name": "warmup",
            "arg_type": "value",
            "required": False,
            "help": "An (optional) specified integer numer of seconds to run a short initial burst of actions to 'warm up' the CPU. Maximum is 60 seconds."
        },
        {
            "map_name": "executor_config",
            "data_type": "integer",
            "data_key": "optimize",
            "arg_name": "--optimize",
            "var_name": "optimize",
            "arg_type": "value",
            "required": False,
            "help": "If specified, Hedra will execute batch size/time optimization for the (integer) number of iterations specified."
        },
        {
            "map_name": "executor_config",
            "data_type": "integer",
            "data_key": "optimizer_iter_duration",
            "arg_name": "--optimizer-iter-duration",
            "var_name": "optimizer_iter_duration",
            "arg_type": "value",
            "required": False,
            "help": "If specified, the amount of time (in integer seconds) each opimization iteration will execute for."
        },
        {
            "map_name": "executor_config",
            "data_type": "string",
            "data_key": "optimizer_type",
            "arg_name": "--optimizer-type",
            "var_name": "optimizer_type",
            "arg_type": "value",
            "required": False,
            "help": "If specified, the type of optimization algorithm Hedra will use to find the best batch size/time values."
        },
        {
            "map_name": "executor_config",
            "data_type": "string",
            "data_key": "save_optimized",
            "arg_name": "--save-optimized",
            "var_name": "save_optimized",
            "arg_type": "value",
            "required": False,
            "help": "If specified, Hedra will save optimized batch size/time values and optimization results to a JSON file at the specified path."
        },
        {
            "map_name": "executor_config",
            "data_type": "integer",
            "data_key": "pool_size",
            "arg_name": "--pool-size",
            "var_name": "pool_size",
            "arg_type": "value",
            "required": False,
            "help": "The number of parallel workers that should be used with either the Local-Parallel or Parallel-Worker runners."
        },
        {
            "map_name": "executor_config",
            "data_type": "boolean",
            "data_key": "no_run_visuals",
            "arg_name": "--no-run-visuals",
            "var_name": "no_run_visuals",
            "arg_type": "flag",
            "required": False,
            "help": "If true, no progress bar will show for run execution (setup and results bars will still show)."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "leader_ips",
            "arg_name": "--leader-ips",
            "var_name": "leader_ips",
            "arg_type": "value",
            "envar_default": "LEADER_IP",
            "required": False,
            "help": "Comma delimited list of IPv4 addresses of Hedra leader instances for workers to connect to - the last IPv4 will be this node's IPv4."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "leader_ports",
            "arg_name": "--leader-ports",
            "var_name": "leader_ports",
            "arg_type": "value",
            "default": "6669",
            "envar_default": "LEADER_PORT",
            "required": False,
            "help": "Comma delimited list of port numbers of Hedra leader instances for workers to connect to - the last port will be this node's port."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "leader_addresses",
            "arg_name": "--leader-addresses",
            "var_name": "leader_addresses",
            "arg_type": "value",
            "envar_default": "LEADER_ADDRESS",
            "required": False ,
            "help": "Comma delimited list of full IPv4 address/port strings of Hedra leader instances for workers to connect to - the last address will be this node's address."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "discovery_mode",
            "arg_name": "--discovery-mode",
            "var_name": "discovery_mode",
            "arg_type": "value",
            "envar_default": "DISCOVERY_MODE",
            "required": False ,
            "help": "String value that determines what method leaders will use to discover each other (static, dynamic, or kubernetes)."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "worker_server_ips",
            "arg_name": "--worker-server-ips",
            "var_name": "worker_server_ips",
            "arg_type": "value",
            "envar_default": "LEADER_IP",
            "required": False,
            "help": "Comma delimited list of IPv4 addresses of Hedra leader instances for workers to connect to - the last IPv4 will be this node's IPv4."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "worker_server_ports",
            "arg_name": "--worker-server-ports",
            "var_name": "worker_server_ports",
            "arg_type": "value",
            "envar_default": "LEADER_PORT",
            "required": False,
            "help": "Comma delimited list of port numbers of Hedra leader instances for workers to connect to - the last port will be this node's port."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "worker_server_addresses",
            "arg_name": "--worker-server-addresses",
            "var_name": "worker_server_addresses",
            "arg_type": "value",
            "envar_default": "LEADER_ADDRESS",
            "required": False ,
            "help": "Comma delimited list of full IPv4 address/port strings of Hedra leader instances for workers to connect to - the last address will be this node's address."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "worker_ip",
            "arg_name": "--worker-ip",
            "var_name": "worker_ip",
            "arg_type": "value",
            "default": "localhost",
            "envar_default": "WORKER_IP",
            "required": False,
            "help": "The IPv4 address of a worker instance, sent back to leaders if live updates are enabled."
        },
        {
            "map_name": "distributed_config",
            "data_type": "integer",
            "data_key": "worker_port",
            "arg_name": "--worker-port",
            "var_name": "worker_port",
            "default": "6670",
            "envar_default": "WORKER_PORT",
            "arg_type": "value",
            "required": False,

            "help": "The port of a worker instance, sent back to leaders if live updates are enabled."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "worker_address",
            "arg_name": "--worker-address",
            "var_name": "worker_address",
            "default": "http://localhost:6670",
            "envar_default": "WORKER_ADDRESS",
            "arg_type": "value",
            "required": False,
            "help": "The IPv4 address and port of a worker instance, sent back to leaders if live updates are enabled." 
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "worker_server_ip",
            "arg_name": "--worker-server-ip",
            "var_name": "worker_server_ip",
            "default": "localhost",
            "envar_default": "WORKER_SERVER_IP",
            "arg_type": "value",
            "required": False,
            "help": "The IPv4 address of a worker updates server instance, sent back to leaders if live updates are enabled."
        },
        {
            "map_name": "distributed_config",
            "data_type": "integer",
            "data_key": "worker_server_port",
            "arg_name": "--worker-server-port",
            "var_name": "worker_server_port",
            "default": "6671",
            "envar_default": "WORKER_SERVER_PORT",
            "arg_type": "value",
            "required": False,

            "help": "The port of a worker updates server instance, sent back to leaders if live updates are enabled."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "worker_server_address",
            "arg_name": "--worker-server-address",
            "var_name": "worker_server_address",
            "envar_default": "WORKER_SERVER_ADDRESS",
            "arg_type": "value",
            "required": False,
            "help": "The IPv4 address and port of a worker updates server instance, sent back to leaders if live updates are enabled." 
        },
        {
            "map_name": "distributed_config",
            "data_type": "integer",
            "data_key": "workers",
            "arg_name": "--workers",
            "var_name": "workers",
            "arg_type": "value",
            "required": False,
            "help": "The number of workers a leader instance should expect to register." 
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "kube_config_filepath",
            "arg_name": "--kube-config-filepath",
            "var_name": "kube_config_filepath",
            "arg_type": "value",
            "envar_default": "KUBE_CONFIG_FILEPATH",
            "required": False ,
            "help": "String value that sets filepath to Kubernetes config for Bootstrap server Broadkast embedded server."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "kube_config_context",
            "arg_name": "--kube-config-context",
            "var_name": "kube_config_context",
            "arg_type": "value",
            "envar_default": "KUBE_CONFIG_CONTEXT",
            "required": False ,
            "help": "String value that sets Kubernetes context for Bootstrap server Broadkast embedded server."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "bootstrap_ip",
            "arg_name": "--bootstrap-server-ip",
            "var_name": "bootstrap_server_ip",
            "default": "localhost",
            "envar_default": "BOOTSTRAP_SERVER_IP",
            "arg_type": "value",
            "required": False,
            "help": "The IPv4 address of a bootstrap server instance."
        },
        {
            "map_name": "distributed_config",
            "data_type": "integer",
            "data_key": "bootstrap_port",
            "arg_name": "--bootstrap-server-port",
            "var_name": "bootstrap_server_port",
            "default": "8711",
            "envar_default": "BOOTSTRAP_SERVER_PORT",
            "arg_type": "value",
            "required": False,
            "help": "The port of a bootstrap server instance."
        },
        {
            "map_name": "distributed_config",
            "data_type": "string",
            "data_key": "bootstrap_address",
            "arg_name": "--bootstrap-server-address",
            "var_name": "bootstrap_server_address",
            "envar_default": "BOOTSTRAP_SERVER_ADDRESS",
            "arg_type": "value",
            "required": False,
            "help": "The IPv4 address and port of a bootstrap server instance." 
        },
        {
            "map_name": "distributed_config",
            "data_type": "integer",
            "data_key": "leaders",
            "arg_name": "--leaders",
            "var_name": "leaders",
            "arg_type": "value",
            "required": False,
            "help": "The number of leaders to be registered." 
        },
        {
            "map_name": "distributed_config",
            "data_type": "integer",
            "data_key": "job_timeout",
            "arg_name": "--job-timeout",
            "var_name": "job_timeout",
            "envar_default": "JOB_TIMEOUT",
            "arg_type": "value",
            "required": False,
            "help": "The number of workers a leader instance should expect to register." 
        },
        {
            "map_name": "embedded_stats",
            "data_type": "boolean",
            "arg_name": "--embedded-stats",
            "var_name": "embedded_stats",
            "arg_type": "flag",
            "required": False,
            "help": "Starts an embedded instance of Statserve prior to any other runners executing."
        },
        {
            "map_name": "distributed_config",
            "data_type": "boolean",
            "data_key": "live_progress_updates",
            "arg_name": "--live-progress-updates",
            "var_name": "live_progress_updates",
            "arg_type": "flag",
            "required": False,
            "help": "Enables the leader or worker's ability to intermittently fetch/calculate the number of currently completed actions."
        },
        {
            "map_name": "as_server",
            "data_type": "boolean",
            "arg_name": "--as-server",
            "var_name": "as_server",
            "arg_type": "flag",
            "required": False,
            "help": "Used only for running hedra in Updates Server of Job Server mode. Bypass normal config options to enable these modes."
        },
        {
            "map_name": "about",
            "data_type": "string",
            "arg_name": "--about",
            "var_name": "about",
            "arg_type": "value",
            "required": False,
            "help": "Describes package version, package description, and lists all args."
        }
    ],
    "config_maps": [
        {
            "map_name": "config",
            "map_type": "value"
        },
        {
            "map_name": "jobs_config",
            "map_type": "dict"
        },
        {
            "map_name": "distributed_config",
            "map_type": "dict"
        },
        {
            "map_name": "reporter_config",
            "map_type": "dict"
        },
        {
            "map_name": "executor_config",
            "map_type": "dict"
        },
        {
            "map_name": "actions",
            "map_type": "value"
        },
        {
            "map_name": "code_actions",
            "map_type": "value"
        },
        {
            "map_name": "as_server",
            "map_type": "value"
        },
        {
            "map_name": "runner_mode",
            "map_type": "value"
        },
        {
            "map_name": "embedded_stats",
            "map_type": "value"
        },
        {
            "map_name": "log_level",
            "map_type": "value"
        },
        {
            "map_name": "about",
            "map_type": "value"
        }
    ],
    "attributes": [
        {
            "name": "reporter_config",
            "type": "dict",
            "default": {}
        },
        {
            "name": "jobs_config",
            "type": "dict",
            "default": {}
        },
        {
            "name": "distributed_config",
            "type": "dict",
            "default": {}
        },
        {
            "name": "executor_config",
            "type": "dict",
            "default": {}
        },
        {
            "name": "actions",
            "type": "dict",
            "default": {}
        },
        {
            "name": "code_actions",
            "type": "list",
            "default": []
        },
        {
            "name": "as_server",
            "type": "dict",
            "default": False
        },
        {
            "name": "runner_mode",
            "type": "string",
            "default": "local"
        },
        {
            "name": "embedded_stats",
            "type": "boolean",
            "default": False
        },
        {
            "name": "log_level",
            "type": "string",
            "default": "info"
        },
        {
            "name": "about",
            "type": "boolean",
            "default": False
        }
    ]
}