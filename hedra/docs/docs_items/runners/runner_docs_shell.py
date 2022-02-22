class RunnerDocsShell:


    def __init__(self) -> None:
        self.runner_types = {
            "local": self._about_local,
            "parallel": self._about_parallel,
            "ephemeral-leader": self._about_leader,
            "ephemeral-worker": self._about_worker,
            "update-server": self._about_update_server,
            "job-leader": self._about_job_leader_sever,
            "job-worker": self._about_job_worker_server,
            "job-server": self._about_job_server
        }


    def about(self, docs_arg):
        return self.runner_types.get(docs_arg)()


    def _about_local(self):
        return '''
        Local Runner - (local)

        key arguments:

        --embedded-statserve <run_embedded_statserve>

        --no-run-visuals <enable_or_disable_progress_bar_for_execution_completion>

        The Local Runner is the default runner for Hedra, and should be used for any testing that does not require 
        significant parallelism. You may also specify the --embedded-statserve argument to run an embedded statserve instance 
        for test results collection/computation/output.

        '''
        
    def _about_parallel(self):
        return '''
        Parallel Runner - (parallel)

        key arguments:

        --pool-size <number_of_processes>

        --embedded-statserve <run_embedded_statserve>

        --no-run-visuals <enable_or_disable_progress_bar_for_execution_completion>

        The Parallel Runner is ideal for maximizing Hedra's throughput/throughput locally or on instances where multiprocesing is 
        available. The Parallel Runner defaults to utilizing the number of physical cores available on the given machine. This 
        is an ideal amount of parallelism, as even with hyperthreading usage of additional cores may only yield limited benefit while 
        requiring significant adjustment of batch size and batch time. You may also specify the --embedded-statserve argument to run 
        an embedded statserve instance for test results collection/computation/output.

        '''

    def _about_leader(self):
        return '''
        Ephemeral Leader Runner - (ephemeral-leader)

        key arguments:

        --leader-ips <comma_delimited_list_of_ipv4_addresses_of_leader_nodes> (the last item will be this node's IPv4)
        
        --leader-ports <comma_delimited_list_of_ports_of_leader_nodes> (the last item will be this node's port)
        
        --leader-addresses <comma_delimited_list_of_full_address_of_leader_nodes> (the last item will be this node's address)

        --workers <integer_number_of_workers_to_wait_for>

        --worker-server-addresses <comma_delimited_list_of_full_ipv4_of_worker_server_addresses>

        --live-progress-updates <enables_updates_to_a_worker_server>

        --job-timeout <timeout_for_ephemeral_job> (default is 600 seconds/10 minutes)

        --bootstrap-server-ip <ipv4_addresses_of_this_bootstrap_node>
        
        --bootstrap-server-ports <port_of_this_bootstrap_node>
        
        --bootstrap-server-addresses <full_addresses_of_this_bootstrap_node>

        --leaders <integer_numer_of_leaders_to_wait_to_register>

        --discovery-mode <string_determining_leader_discovery_method>

        The Ephemeral Leader Runner allows for distributed execution across a network of computers (kubernetes cluster, etc.). The leader
        will wait for the specified number of workers to register, close registration, submit partitioned batch sizes and execution configs 
        to the registered workers based on the cli arguments/environmental values provided, and beginning execution. The leader will then 
        wait for all workers to finish before computing combined results and submitting to the specified reporter resrouces.

        Leaders will regularly verify all connected/specified workers are healthy - if a worker fails, it will be evicted from any currently 
        running jobs but available for use in any future jobs. If worker addresses are specified via the `--worker-server-addresses` argument, 
        leaders will automatically connect/re-connect to any workers at the addresses  specified should they fail, otherwise workers must 
        be provided leader addresses to reconnect.

        Leaders can also discover one another dynamically when supplied the the ipv4/port(s) or address(s) of running Bootstrap Server
        instances via the bootstrap-server-ip/boot-strap-server-port or bootstrap-server-address arguments AND the number of leaders to
        wait for via the leaders argument. If you are running Hedra on a non-Kubernetes cluster, we recommend running a single Bootstrap
        Server and selecting "dynamic" for the --discovery-mode argument. If you are running Hedra in a Kubernetes cluster, we recommend
        running multiple Bootstrap servers and selecting "kubernetes" argument, which utilizes the Broadkast library to
        discovery Hedra leader nodes.
        
        NOTE: Dynamic leader discovery is still being developed and should not be used in production.
        
        '''

    def _about_worker(self):
        return '''
        Ephemeral Worker Runner - (ephemeral-worker)

        key arguments:

        --leader-ips <comma_delimited_list_of_ipv4_addresses_of_leader_nodes>
        
        --leader-ports <comma_delimited_list_of_ports_of_leader_nodes>
        
        --leader-addresses <comma_delimited_list_offull_address_of_leader_nodes>

        --worker-ip <ipv4_of_worker_node_for_live_updates>

        --worker-port <port_of_worker_node_for_live_updates>

        --worker-address <full_ipv4_of_worker_node_for_live_updates>

        --live-progress-updates <enables_updates_to_a_worker_server>

        --no-run-visuals <enable_or_disable_progress_bar_for_execution_completion>

        --worker-server-ip <ipv4_address_of_worker_server>

        --worker-server-port <port_of_worker_server>

        --worker-server-address <full_ipv4_of_worker_server>

        The Ephemeral Worker Runner allows for distributed execution across a network of computers (kubernetes cluster, etc.). When a
        worker initializes, it will register with all leaders provided at their specified address. Once all workers have registered 
        the leader will send back a partitioned batch size and runner configuration to each worker, beginning execution. Upon 
        completing execution, the worker will submit statistics and results for all actions to the resource specified in reporter 
        config before exiting.

        Like the Local runner, workers make use of pipelines, synchronizing with leaders at each stage to ensure all workers progress 
        uniformly - thus ensuring a cluster of workers will generate the most load upon the target system possible. Workers will 
        regularly verify all connected/specified leaders are healthy - if a leader fails, workers will temporarily evict that leader
        temporarily until it re-registers. If leader addresses are specified via the `--leader-addresses` argument, workers will 
        automatically re-connect to any leaders at the addresses  specified should they fail, otherwise leaders must be provided
        worker addresses to reconnect.

        '''

    def _about_update_server(self):
        return '''
        Update Server - (update-server)

        key arguments:

        --leader-ips <comma_delimited_list_of_ipv4_addresses_of_leader_nodes>
        
        --leader-ports <comma_delimited_list_of_ports_of_leader_nodes>
        
        --leader-addresses <comma_delimited_list_offull_address_of_leader_nodes>

        --worker-server-ip <ipv4_address_of_worker_server>

        --worker-server-port <port_of_worker_server>

        --worker-server-address <full_ipv4_of_worker_server>

        --as-server <run_as_worker_server> (required)

        The Update Server does not execute any actions, but facilitates retrieval of running statistics for a distributed job running 
        over given leader/worker nodes via REST api. The Update Server connects to the specified leader by sending the leader its 
        ipv4/port/address. The leader will then request updates from workers at regular interval (about once every seconds) as to how many 
        total actions have been completed among all workers.

        You may retrieve results from running jobs or instances at any time via GET request to:

            https(s)://<updates_sever_ip_and_port>/api/hedra/workers/updates

        This request will return the:

            - total actions completed
            - time elapsed since testing started
            - current actions-per-second (APS) rate

        for all leader instances to which the Update Server is connected. Note if a configuration has multiple leaders, the same results will
        appear for each leader. Also note that these statistics will only update after each batch is completed - thus high batch times may 
        result in statistics "lagging" behind their actual current values.

        '''

    def _about_job_leader_sever(self):
        return '''
        Leader Job Server - (job-leader)

        key arguments:

        --leader-ips <comma_delimited_list_of_ipv4_addresses_of_leader_nodes> (the last item will be this node's IPv4)
        
        --leader-ports <comma_delimited_list_of_ports_of_leader_nodes> (the last item will be this node's port)
        
        --leader-addresses <comma_delimited_list_offull_address_of_leader_nodes> (the last item will be this node's address)

        --workers <integer_number_of_workers_to_wait_for>

        --live-progress-updates <enables_updates_to_a_worker_server>

        The Leader Job Server does not execute actions (like the regular Leader). However, unlike the regular Leader, the Leader Job Server
        does not exit upon all registered workers completing. Instead, it continues to run - dispactching and assigning jobs to registered
        Worker Job Server runners as jobs are submmited via a Jobs Server runner REST API instance.

        Leaders will regularly verify all connected/specified workers are healthy - if a worker fails, it will be evicted from any currently 
        running jobs but available for use in any future jobs. If worker addresses are specified via the `--worker-server-addresses` argument, 
        leaders will automatically connect/re-connect to any workers at the addresses  specified should they fail, otherwise workers must 
        be provided worker addresses to reconnect.
        
        '''

    def _about_job_worker_server(Self):
        return '''
        Worker Job Server - (job-worker)

        key arguments:

        --leader-ips <comma_delimited_list_of_ipv4_addresses_of_leader_nodes>
        
        --leader-ports <comma_delimited_list_of_ports_of_leader_nodes>
        
        --leader-addresses <comma_delimited_list_offull_address_of_leader_nodes>

        --worker-ip <ipv4_of_worker_node_for_live_updates>

        --worker-port <port_of_worker_node_for_live_updates>

        --worker-address <full_ipv4_of_worker_node_for_live_updates>

        --live-progress-updates <enables_updates_to_a_worker_server>

        --no-run-visuals <enable_or_disable_progress_bar_for_execution_completion>

        --worker-server-ip <ipv4_address_of_worker_server>

        --worker-server-port <port_of_worker_server>

        --worker-server-address <full_ipv4_of_worker_server>

        The Worker Job Server allows for distributed execution across a network of computers (kubernetes cluster, etc.). When a
        worker job server initializes, it will register with all leaders provided at their specified address. Once all workers have 
        registered the leader will send back a partitioned batch size and runner configuration to each worker, beginning execution. 
        Upon  completing execution, the worker will submit statistics and results for all actions to the resource specified in reporter
        config. Like the Local runner, workers make use of pipelines, synchronizing with leaders at each stage to ensure all workers 
        progress uniformly - thus ensuring a cluster of workers will generate the most load upon the target system possible.
        
        Unlike the normal Worker, the Worker Job Server will not exit upon completion, and can execute multiple jobs at once. Note that 
        too many simultaneously executing jobs will hurt each running job's performance. Also note that you cannot re-submit a job
        with the same job name or cancel a job once submitted - it must either complete all provided stages or error. You can run as
        many differently-named jobs as desired.

        Like the Local runner, workers make use of pipelines, synchronizing with leaders at each stage to ensure all workers progress 
        uniformly - thus ensuring a cluster of workers will generate the most load upon the target system possible. Workers will 
        regularly verify all connected/specified leaders are healthy - if a leader fails, workers will temporarily evict that leader
        temporarily until it re-registers. If leader addresses are specified via the `--leader-addresses` argument, workers will 
        automatically re-connect to any leaders at the addresses  specified should they fail, otherwise leaders must be provided
        worker addresses to reconnect.

        '''

    def _about_job_server(self):
        return '''
        Job Server - (job-server)

        key arguments:

        --leader-ips <comma_delimited_list_of_ipv4_addresses_of_leader_nodes>
        
        --leader-ports <comma_delimited_list_of_ports_of_leader_nodes>
        
        --leader-addresses <comma_delimited_list_offull_address_of_leader_nodes>

        The Job Server does not execute actions. Instead, it runs an async Flask server that serves as a reverse-proxy to the specified
        leaders, which allows for submitting jobs to Leader Job Server instances that will then distribute and schedule the submitted
        job to registered running by Worker Job Servers. Jobs may be created or updated via POST or PUT request to:

            http(s)://<job_server_ip_and_port>/api/hedra/jobs/setup

        Note that jobs will *not* run when created. You may also retrieve jobs via GET request to:
        
            http(s)://<job_server_ip_and_port>/api/hedra/jobs/get?job=<job_name>
        
        and delete jobs via DELETE request to:

            http(s)://<job_server_ip_and_port>/api/hedra/jobs/delete?job=<job_name>

        Finally, you may start jobs via GET request to:

            http(s)://<job_server_ip_and_port>/api/hedra/jobs/start?job=<job_name>

        Note that the request may take a few seconds to complete for particularly large batch sizes (greater than 120k). Jobs can be 
        specified as:

            {
                'executor_config': {
                    ...<any_valid_executor_configuration_values>
                },
                'actions': {
                    ...<actions_specified_as_per_persona_in_executor_config>
                }
            }

        '''

    def _about_bootstrap_server(self):
        return '''
        Bootstrap Server - (bootstrap-server)

        key arguments:
        
        --bootstrap-server-ip <ipv4_addresses_of_this_bootstrap_node>
        
        --bootstrap-server-ports <port_of_this_bootstrap_node>
        
        --bootstrap-server-addresses <full_addresses_of_this_bootstrap_node>

        --leaders <integer_numer_of_leaders_to_wait_to_register>

        The Bootstrap Server allows hedra leaders to discover and connect to one another by allowing a specified
        number of leaders to register, then sending all registered leaders the addresses of leader nodes registered.
        
        NOTE: The Bootstrap Server and dynamic leader discovery is still in development. We do not recommend using
        this feature in production environments.
        '''

    