
# <b>Hedra - Testing at scale </b>
[![PyPI version](https://img.shields.io/pypi/v/hedra?color=gre)](https://pypi.org/project/hedra/)
[![License](https://img.shields.io/github/license/scorbettUM/hedra)](https://github.com/scorbettUM/hedra/blob/main/LICENSE)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](https://github.com/scorbettUM/hedra/blob/main/CODE_OF_CONDUCT.md)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/hedra)](https://pypi.org/project/hedra/)


| Package     | Hedra                                                           |
| ----------- | -----------                                                     |
| Version     | 0.6.16                                                          |
| Web         | TBD                                                             |
| Download    | https://pypi.org/project/hedra/                                 | 
| Source      | https://github.com/scorbettUM/hedra                             |
| Keywords    | performance, testing, async, distributed, graph, DAG, workflow  |

Hedra is a Python performance and scalable unit/integration testing framework that makes creating and running complex test workflows easy.

These workflows are written as directed acrylic graphs in Python, where each graph is specified as a collection of Python classes referred to as <b>stages</b>. Each Stage may then specify async Python methods which are then wrapped in Python decorators (referred to as <b>hooks</b>), which that Stage will then execute. The hook wrapping a method tells Hedra both what the action does and when to execute it. In combination, stages and hooks allow you to craft test workflows that can mimic real-world user behavior, optimize framework performance, or interact with a variety of Hedra's powerful integrations.

<br/>

___________

## <b> Why Hedra? </b>

Understanding how your application performs under load can provide valuable insights - allowing you to spot issues with latency, memory usage, and stability. However, performance test tools providing these insights are often difficult to use and lack the ability to simulate complex user interaction at scale. Hedra was built to solve these problems by allowing developers and test engineers to author performance tests as sophisticated and scalable workflows. Hedra adheres to the following tenants:

<br/>

### __Speed and efficiency by default__ 

Regardless of whether running on your personal laptop or distributed across a cluster, Hedra is *fast*, capable of generating millions of requests or interactions per minute and without consuming excessive memory. Hedra pushes the limits of Python to achieve this, embracing the latest in Python async and multiprocessing language features to achieve optimal execution performance.

<br/>

### __Run with ease anywhere__

Hedra is simple to set up regardless of how you're choosing to run it, and authoring/managing/running test workflows is easy. Hedra includes integrations with Git to facilitate easy management of collections of graphs via <b>Projects</b>, the ability to generate flexible starter test templates, and an API that is both fast and intuitive to understand. Distributed use almost exactly mirrors local operation, reducing the learning curve for more complex deployments.

<br/>

### __Painless flexibility and extensibility__
Hedra ships with support for HTTP, HTTP2, Websockets, and UDP out of the box. GraphQL, GRPC, and Playwright are available simply by installing the (optional) dependency packages. Hedra offers JSON and CSV results output by default, with 28 additional results reporting options readily available by likewise installing the required dependencies.

Likewise, Hedra offers a comprehensive plugin system. You can easily write a plugin to test your Postgresql database or integrate a third party service, with CLI-generated templates to guide you and full type hints support throughout. Unlike other frameworks, no additional compilation or build steps are required - just write your plugin, import it, and include it in the appropriate Stage in your test graph.

<br/>

___________

## <b>Requirements and Getting Started</b>

Hedra has been tested on Python versions 3.8.6+, though we recommend using Python 3.10+. You should likewise have the latest LTS version of OpenSSL, build-essential, and other common Unix dependencies installed (if running on a Unix-based OS).

*<b>Warning</b>*: Hedra has currently only been tested on the latest LTS versions of Ubuntu and Debian. Other official OS support is coming Mar. 2023. 

<br/>

### __Installing__ 

To install Hedra run:
```
pip install hedra
```
Verify the installation was was successful by running the command:
```
hedra --help
```

which should output

![Output of the hedra --help command](https://github.com/scorbettUM/hedra/blob/main/images/hedra_help_output.png?raw=true "Verifyin Install")

<br/>

### __Creating your first graph__ 

Get started by running Hedra's:
```
hedra graph create <path/to/graph_name.py>
```
command in an empty directory to generate a basic test from a template. For example, run:
```
hedra graph create example.py
```
which will output the following:

![Output of the hedra graph create example.py command](https://github.com/scorbettUM/hedra/blob/main/images/hedra_graph_create.png?raw=true "Creating a Graph")

and generate the the test below in the specified `example.py` file:
```python
from hedra import (
	Setup,
	Execute,
	action,
	Analyze,
	JSONConfig,
	Submit,
	depends,
)

class SetupStage(Setup):
    batch_size=1000
    total_time='1m'


@depends(SetupStage)
class ExecuteHTTPStage(Execute):

    @action()
    async def http_get(self):
        return await self.client.http.get('https://<url_here>')


@depends(ExecuteHTTPStage)
class AnalyzeStage(Analyze):
    pass


@depends(AnalyzeStage)
class SubmitJSONResultsStage(Submit):
    config=JSONConfig(
        events_filepath='./events.json',
        metrics_filepath='./metrics.json'
    )

```

We'll explain this graph below, but for now  - replace the string `'https://<url_here>'` with `'https://httpbin.org/get'`.

<br/>
Before running our test, if on a Unix system, we may need to set the maximum number of open files above its current limit. This can be done
by running:

```
ulimit -n 256000
```

note that you can provide any number here, as long as it is greater than the `batch_size` specified in the `SetupStage` Stage. With that, we're ready run our first test by executing:
```
hedra graph run example.py
```

Hedra will load the test graph file, parse/validate/setup the stages specified, then begin executing your test:

![Output of the hedra graph run example.py command](https://github.com/scorbettUM/hedra/blob/main/images/hedra_graph_run_example.png?raw=true "Running a Graph")

The test will take a minute or two to run, but once complete you should see:

![Output of hedra from a completed graph run](https://github.com/scorbettUM/hedra/blob/main/images/hedra_graph_complete.png?raw=true "A Complete Graph Run")

You have officially created and run your first test graph!

<br/>


___________

## <b>Development</b>

Local development requires at-minimum Python 3.8.6, though 3.10.0+ is recommended. To setup your environment run:

```
python3 -m venv ~/.hedra && \
source ~/.hedra/bin/activate && \
git clone https://github.com/scorbettUM/hedra.git && \
cd hedra && \
pip install --no-cache -r requirements.in && \
python setup.py develop
```

To develop or work with any of the additional provided engines, references the dependency tables below.

<br/>

___________

## <b>Engines, Personas, Algorithms, and Reporters</b>

Much of Hedra's extensibility comes in the form of both extensive integrations/options and plugin capabilities for four main framework features:
<br/>

### __Engines__ 
Engines are the underlying protocol or library integrations required for Hedra to performance test your application (for example HTTP, UDP, Playwright). Hedra currently supports the following Engines, with additional install requirements shown if necessary:

| Engine        | Additional Install Option                                       |  Dependencies                 |
| -----------   | -----------                                                     |------------                   |
| HTTP          | N/A                                                             | N/A                           |
| HTTP2         | N/A                                                             | h2, hpack                     |
| UDP           | N/A                                                             | N/A                           |
| Websocket     | N/A                                                             | N/A                           |
| GRPC          | pip install hedra[grpc]                                         | grpcio grpco-tools, h2, hpack |
| GraphQL       | pip install hedra[graphql]                                      | gql                           |
| GraphQL-HTTP2 | pip install hedra[graphql]                                      | gql, h2, hpack                |
| Playwright    | pip install hedra[playwright] && playwright install             | playwright                    |


<br/>

### __Personas__

Personas are responsible for scheduling when `@action()` or `@task()` hooks execute over the specified Execute stage's test duration. No additional install dependencies are required for Personas, and the following personas are currently supported out-of-box:

| Persona               | Setup Config Name       | Description                                                                                                                                                                                                    |
| ----------            | ----------------        | -----------------                                                                                                                                                                                              |
| Batched               | batched                 | Executes each action or task hook in batches of the specified size, with an optional wait between each batch spawning                                                                                          |
| Constant Arrival Rate | constant-arrival        | Hedra automatically adjusts the batch size after each batch spawns based upon the number of hooks that have completed, attempting to achieve `batch_size` completions per batch                                |
| Constant Spawn Rate   | constant-spawn          | Like `Batched`, but cycles through actions before waiting `batch_interval` time.                                                                                                                               |
| Default               | N/A                     | Cycles through all action/task hooks in the Execute stage, resulting in a (mostly) even distribution of execution                                                                                              |
| No-Wait               | no-wait                 | Cycles through all action/task hooks in the Execute stage, resulting in a (mostly) even distribution of execution. __WARNING__: This persona may cause OOM as it does not use the memory management algorithm. |    
| Ramped                | ramped                  | Starts at a batch size of  `batch_gradient` * `batch_size`. Batch size increases by the gradient each batch with an optional wait between each batch spawning                                                  |
| Ramped Interval       | ramped-interval         | Executes `batch_size` hooks before waiting `batch_gradient` * `batch_interval` time. Interval increases by the gradient each batch                                                                             |
| Sorted                | sorted                  | Executes each action/task hook in batches of the specified size and in the order provided to each hook's (optional) `order` parameter                                                                          |
| Weighted              | weighted                | Executes action/task hooks in batches of the specified size, with each batch being generated from a sampled distribution based upon that action's weight                                                       |

<br/>

### __Algorithms__

Algorithms are used by Hedra `Optimize` stages to calculate maximal test config options like `batch_size`, `batch_gradient`, and/or `batch_interval`. All out-of-box supported algorithms use `scikit-learn` and include:

| Algorithm               | Setup Config Name   | Description                                                                           |
| ----------              | ----------------    | -----------------                                                                     |
| SHG                     | shg                 | Uses `scikit-learn`'s Simple Global Homology (SHGO) global optimization algorithm     |
| Dual Annealing          | dual-annealing      | Uses `scikit-learn`'s Dual Annealing global optimization algorithm                    |
| Differential Evolution  | diff-evolution      | Uses `scikit-learn`'s Differential Evolution global optimization algorithm            |

<br/>

### __Reporters__

Reporters are the integrations Hedra uses for submitting aggregated and unaggregated results (for example, to a MySQL database via the MySQL reporter). Hedra currently supports the following Reporters, with additional install requirements shown if necessary:

| Engine               | Additional Install Option                                 |  Dependencies                            |
| -----------          | -----------                                               |------------                              |
| AWS Lambda           | pip install hedra[aws]                                    | boto3                                    |
| AWS Timestream       | pip install hedra[aws]                                    | boto3                                    |
| Big Query            | pip install hedra[google]                                 | google-cloud-bigquery                    |
| Big Table            | pip install hedra[google]                                 | google-cloud-bigtable                    |
| Cassandra            | pip install hedra[cassandra]                              | cassandra-driver                         |
| Cloudwatch           | pip install hedra[aws]                                    | boto3                                    |
| CosmosDB             | pip install hedra[azure]                                  | azure-cosmos                             |
| CSV                  | N/A                                                       | N/A                                      |
| Datadog              | pip install hedra[datadog]                                | datadog                                  |
| DogStatsD            | pip install hedra[statsd]                                 | aio_statsd                               |
| Google Cloud Storage | pip install hedra[google]                                 | google-cloud-storage                     |
| Graphite             | pip install hedra[statsd]                                 | aio_statsd                               |
| Honeycomb            | pip install hedra[honeycomb]                              | libhoney                                 |
| InfluxDB             | pip install hedra[influxdb]                               | influxdb_client                          |
| JSON                 | N/A                                                       | N/A                                      |
| Kafka                | pip install hedra[kafka]                                  | aiokafka                                 |
| MongoDB              | pip install hedra[mongodb]                                | motor                                    |
| MySQL                | pip install hedra[sql]                                    | aiomysql, sqlalchemy                     |
| NetData              | pip install hedra[statsd]                                 | aio_statsd                               |
| New Relic            | pip install hedra[newrelic]                               | newrelic                                 |
| Postgresql           | pip install hedra[sql]                                    | aiopg, psycopg2-binary, sqlalchemy       |
| Prometheus           | pip install hedra[prometheus]                             | prometheus-client, prometheus-client-api |
| Redis                | pip install hedra[redis]                                  | redis, aioredis                          |
| S3                   | pip install hedra[aws]                                    | boto3                                    |
| Snowflake            | pip install hedra[snowflake]                              | snowflake-connector-python, sqlalchemy   |
| SQLite3              | pip install hedra[sql]                                    | sqlalchemy                               |
| StatsD               | pip install hedra[statsd]                                 | aio_statsd                               |
| Telegraf             | pip install hedra[statsd]                                 | aio_statsd                               |
| TelegrafStatsD       | pip install hedra[statsd]                                 | aio_statsd                               |
| TimescaleDB          | pip install hedra[sql]                                    | aiopg, psycopg2-binary, sqlalchemy       |

<br/>

___________

## <b>Resources</b>

Hedra's official and full documentation is currently being written and will be linked here soon!

___________

## <b>License</b>

This software is licensed under the MIT License. See the LICENSE file in the top distribution directory for the full license text.

___________

## <b>Contributing</b>

Hedra will be open to general contributions starting Fall, 2023 (once the distributed rewrite and general testing is complete). Until then, feel
free to use Hedra on your local machine and report any bugs or issues you find!

___________

## <b>Code of Conduct</b>

Hedra has adopted and follows the [Contributor Covenant code of conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/code_of_conduct.md).
If you observe behavior that violates those rules please report to:

| Name            | Email                       | Twitter                                      |
|-------          |--------                     |----------                                    |
| Sean Corbett    | sean.corbett@umontana.edu   | [@sc_codeum](https://twitter.com/sc_codeUM/) |