# Selectosaur


1. The query to run for each host
select max(usage), min(usage) from cpu_usage where host = 'host_000008' and ts between '2017-01-01 08:59:22' and '2017-01-01 09:59:22';
2. Worker pattern in Go
3. DB connection (go driver? postgres or timescale db?)
4. timescale intro docs + go connection docs (https://docs-dev.timescale.com/docs-add-visualizing-chunks-tutorial/timescaledb/add-visualizing-chunks-tutorial/quick-start/golang/#prerequisites)
5. Readme doc + process to compile & run the app (single command)
6. Some basic memory & CPU analysis using any profilers
7. polish the public interface of the package

db connection string
`postgres://tsdbadmin:ha43nao4zo8ssg17@ixseujmyj1.rmdomcteja.tsdb.cloud.timescale.com:31703/tsdb?sslmode=require`

`select max(usage), min(usage) from cpu_usage where host = 'host_000008' and ts between '2017-01-01 08:59:22' and '2017-01-01 09:59:22';`

```
select date_trunc('minute', ts) as clock, max(usage), min(usage) from cpu_usage where host = 'host_000008' and ts between '2017-01-01 08:59:22' and '2017-01-01 09:59:22' group by clock;

select time_bucket('1 minute', ts) as clock, max(usage), min(usage) from cpu_usage where host = 'host_000008' and ts between '2017-01-01 08:59:22' and '2017-01-01 09:10:22' group by clock;

select time_bucket('1 minute', ts) + '22 seconds' as clock, max(usage), min(usage) from cpu_usage where host = 'host_000008' and ts between '2017-01-01 08:59:22' and '2017-01-01 09:12:22' group by clock;
```