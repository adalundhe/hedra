class HandlerDocs:

    @classmethod
    def about(cls):
        return '''
        Results

        key-arguments:

        --config-filepath <path_to_config_JSON_file> (defaults to the directory in which hedra is running)

        --reporter <reporter_type_to_use> (i.e. datadog, postgres, etc.)

        Hedra offers powerful features for aggregating and storing test results. These features can be broken down
        into three main parts:

        - Events -  the unaggregated results from test execution. Each event contains individual action timing
                    measurement, success/fail status, and additional useful context such as target url, environment, etc.

        - Metrics - Aggregated statistics calculated from the timing measurements of each Action. Although a given action may have many "events" 
                    (occurrences/executions), each action has only *one* of a given Metric type (avg. time, pass/fail counts)

        - Reporters - Integrations with various popular data storage/computational services (for example, Postgres, S3, Prometheus)
                      where Metrics are submitted for storage.


        Test results are processed as follows:

        1. The result of an action is converted a Hedra Event.
        
        2. Each event is submitted for aggregation to a local StatStream instance.

        3. Once all events are submitted, Hedra merges any aggregated statistics from independent threads or distributed workers. 
        
        4. Each merged statstic is converted to a Metric (for example, avg. execution time).

        5. Hedra will submit these metrics for final storage to the location/service specified via reporter type and reporter config.


        Hedra will automatically process results at the end of execution based on the configuration stored in
        and provided by the config.json or in a configuration class inheriting the Test() class. For example:

        {
            "reporter_config": {
                "save_to_file": true
            }
        }

        will tell Hedra to output a JSON file of aggregated metrics at the end.

        Depdending on the executor seleted, Hedra will either attempt to connect to the specified reporter resources
        prior to execution or (specifically for the parallel executor) after execution has completed. 
        
        
        For more information on Reporters, run the command:

            hedra --about results:reporting

        For more information on Events, run the command:

            hedra --about results:events

        For more information on Metrics, run the command:

            hedra --about results:metrics
            

        Related Topics:

        - runners
        
        '''