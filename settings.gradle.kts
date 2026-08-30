rootProject.name = "wiggle"

include(
    "core",
    "proto",
    "coordinator:spi",
    "coordinator:runtime",
    "coordinator:etcd",
    "server",
    "jdbc",
    "postgres",
    "mysql",
    "oracle",
    "sqlserver",
    "cassandra",
    "client",
    "dist",
    "example",
    "tests",
    "cli"
)
