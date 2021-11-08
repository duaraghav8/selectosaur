# Selectosaur

Selectosaur is a CLI tool. It runs SQL queries on Timescale DB based on user-supplied parameters and outputs stats for them.

## Build
It is recommended that you build this app on a 64-bit Mac or Linux machine with Golang version `1.17`.

1. Clone this repo on to your system
2. Navigate to the root directory of this project and run `go build` to generate the binary. Alternatively, download a binary suitable for your platform from the [Releases](https://github.com/duaraghav8/selectosaur/releases) page.
3. Ensure that the binary has execution permissions on your system. For Linux/MacOS, you can run `chmod +x ./selectosaur`.
4. Test the binary by invoking `./selectosaur -h`. This should display the main help message of the CLI.

## Run
1. Ensure that your query params CSV file `query_params.csv` is in the same directory as the CLI.
2. Run the below commands:

```shell
# NOTE: this is a temp timescale DB cluster which will be taken down soon. Enjoy for now.
export DB_CONNECTION_STRING="postgres://tsdbadmin:ha43nao4zo8ssg17@ixseujmyj1.rmdomcteja.tsdb.cloud.timescale.com:31703/tsdb?sslmode=require"

$ ./selectosaur --qp query_params.csv --worker-count 10

    Total number of queries run:      200
    Number of failures:               0
    Total time across all queries:    329.820000 ms
    Average query time:               1.649100 ms
    Minimum query time:               1.349000 ms
    Maximum query time:               2.998000 ms
    Median query time:                1.462000 ms

```