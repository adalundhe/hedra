hedra_uwsgi_cli = {
    "name": "Hedra - API Server",
    "description": "API service for retrieving info on running load tests and running stats or running Hedra as a jobs server.",
    "arguments": [
        {
            "map_name": "uwsgi_ini_path",
            "data_type": "string",
            "arg_name": "--uwsgi-ini-path",
            "var_name": "uwsgi_ini_path",
            "arg_type": "value",
            "envar_default": "UWSGI_INI_PATH",
            "required": False,
            "help": "Specifies the path to the uwsgi.ini file."
        },
        {
            "map_name": "about",
            "data_type": "boolean",
            "arg_name": "--about",
            "var_name": "about",
            "arg_type": "flag",
            "required": False,
            "help": "Describes package version, package description, and lists all args."
        }
    ],
    "config_maps": [
        {
            "map_name": "uwsgi_ini_path",
            "map_type": "value"
        },
        {
            "map_name": "about",
            "map_type": "value"
        }
    ],
    "attributes": [
        {
            "name": "uwsgi_ini_path",
            "type": "string",
            "default": None
        },
        {
            "name": "about",
            "type": "boolean",
            "default": False
        }
    ]
}