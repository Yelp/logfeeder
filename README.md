# LogFeeder
Reads log data from an API and feeds it to various outputs (Amazon SQS or Elasticsearch).

*WARNING*: Never use production data for testing LogFeeder as it will output all sensitive data. Only use production data in a secured environment.

LogFeeder provides 2 output and 1 input plugin implentation.

* SqsOutput - provides the interface to write the output out to an Amazon Simple Queue Service (SQS) queue. For more information see: https://aws.amazon.com/documentation/sqs/

* ESOutput - provides the interface to write the output out to ElasticSearch. For more information see: https://www.elastic.co/guide/index.html

* S3Feeder - provides an interface to read files resident on an S3 bucket. It handles S3 event notifications to retrieve a list of bucket and key pairs. The account will need `s3:GetObject` and `s3:ListBucket` permissions on the S3 bucket. For more information about S3 event notifications see: http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html


## Config Files
Configuration files are located under the `configs` directory.

LogFeeder requires a main configuration file that is specified through the `-c` flag as a command-line argument (by default configuration file should be located in `configs/logfeeder_config.yaml`). This configuration file contains configuration information for logging to scribe via clog, general settings for LogFeeder (e.g. the domain and path to the AWS credentials file), and links to configuration files for each LogFeeder application (e.g. Duo, Salesforce).

Each application has it's own configuration file that is specified in the main LogFeeder configuration file. These configuration files should contain the following:
- `api_creds_filepath` (REQUIRED): the path to the API credentials file, relative to the main LogFeeder directory (e.g. credentials/duo_creds.yaml)
- `rate_limiter_num_calls_per_timeunit` (OPTIONAL): If the API is rate limited, you should specify the max number of calls that can be made to the API in a timeunit (i.e. period of time)
- `rate_limiter_num_seconds_per_timeunit` (OPTIONAL): If the API is rate limited, you should specify the timeunit (e.g. 30 seconds) during which some max number of calls to the API can be made.
- `sub_api boolean switches` (OPTIONAL): If your API has different sub_apis (see DuoFeeder & GoogleFeeder for examples), you should specify which sub_apis to enable. The names of these switches MUST be in the format `enable_{SUBAPI_NAME}` where `{SUBAPI_NAME}` is the name of the sub_api (e.g. `enable_auth`)

## Credential Files
Credentials files are located under the `credentials` directory.

A file with AWS SQS credentials MUST be provided. This file should be specified in the main LogFeeder configuration file under `logfeeder.aws_config_filepath`. See [example_credentials/aws_info.yaml.example](example_credentials/aws_info.yaml.example) for the file format.

Additionally, a file should be provided with credentials to authenticate LogFeeder to an application's API. This file is specified in the application-specific configuration file, under the key `api_creds_filepath`. This file may be in any format since the `read_api_creds()` method, responsible for reading this file, will be overwritten in each application's subclass of LogFeeder.

## Last Timestamp Files
Last Timestamp Files are stored under the `log_files` directory.

Last timestamp files are created to indicate the timestamp of the latest API record received during a run of LogFeeder for a particular (sub_)api. These files are used to determine what start time to use for a subsequent run of LogFeeder.

## Lock Files
Lock Files are created under the `locks` directory.

Lock files are automatically created by LogFeeder to ensure that multiple instances of a subclass of LogFeeder do not run simultaneously.

## Errors
LogFeeder logs errors to scribe including uncaught exceptions. Using the provided configuration file, only messages with a log level of `WARN` or higher are logged.

However, if LogFeeder encounters an error prior to the initialization of the logging subsystem, the error is printed out to stderr. If you intend to run it as a service you should redirect the stderr output to a log file so you can view the error logs in this scenario.

## Running LogFeeder
Follow these steps to run logfeeder:

1. Clone the Git repository.
1. Obtain credentials to the service you intend to ingest logs for.
    1. For the proper format of these credentials files, see example files in the `log_feeder/example_credentials` directory.
    1. Place these credentials in the `log_feeder/credentials` directory and set the application configuration file (e.g. `log_feeder/configs/duo_config.yaml`) to point to this credentials file.

### Tips for running LogFeeder
* To see command-line options, run:
```shell
$ python -m log_feeder.duo_feeder -h
```
* Check out the files in `log_feeder/configs` for configuration options
* If you change the LogFeeder source code, make sure all the tests pass:
```shell
$ make test
```
* LogFeeder by default starts its query at the latest timestamp of its last run. Logfeeder will generate duplicate
entries and expects the output system (e.g. Elasticsearch/SQS) to deal with them.

## Development Environment
Use tox to build a reliable virtualenv:
```shell
$ make venv
$ source venv-logfeeder/bin/activate
```

## Tests
Use tox to run the test suite:
```shell
$ make test
```
