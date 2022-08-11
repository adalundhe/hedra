from hedra.command_line import CommandLine
from hedra.core.personas.parallel import calculate_total_actions


def generate_configs(pool_size, cli):
    configs = []

    total_actions = calculate_total_actions(
            total_actions=cli.executor_config.get('total_actions'),
            total_time=cli.executor_config.get('total_time'),
            batch_size=cli.executor_config.get('batch_size')
        )

    pool_bin_size = int(total_actions/pool_size)
    last_bin_size = pool_bin_size

    if total_actions % pool_size > 0:
        last_bin_size = total_actions % pool_size

    for _ in range(pool_size):
        command_line = CommandLine()
        command_line = command_line.copy(cli)
        command_line.executor_config['total_actions'] = pool_bin_size
        
        configs.append(command_line)

    configs[len(configs)-1].executor_config['total_actions'] = last_bin_size

    return configs
