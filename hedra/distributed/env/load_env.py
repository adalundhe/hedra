import os
from dotenv import dotenv_values
from typing import (
    Dict, 
    Union, 
    Type,
    TypeVar
)
from .env import Env
from .monitor_env import MonitorEnv
from .replication_env import ReplicationEnv
from .registrar_env import RegistrarEnv

T = TypeVar('T')


PrimaryType=Union[str, int, bool, float, bytes]


def load_env(
    env: Type[T],  
    env_file: str=None
) -> T:
    
    env_type: Union[Env, MonitorEnv, ReplicationEnv, RegistrarEnv] = env
    envars = env_type.types_map()
    
    if env_file is None:
        env_file = '.env'

    values: Dict[str, PrimaryType] = {}

    for envar_name, envar_type in envars.items():
        envar_value = os.getenv(envar_name)
        if envar_value:
            values[envar_name] = envar_type(envar_value)

    if env_file and os.path.exists(env_file):
        env_file_values = dotenv_values(dotenv_path=env_file)

        for envar_name, envar_value in env_file_values.items():
            envar_type = envars.get(envar_name)
            if envar_type:
                env_file_values[envar_name] = envar_type(envar_value)

        values.update(env_file_values)

    return env(**{
        name: value for name, value in values.items() if value is not None
    })
