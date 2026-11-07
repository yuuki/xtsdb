# Security policy

xtsdb is a network service: it accepts Prometheus Remote Write and Graphite
data from the network, serves queries over HTTP, and holds credentials for
Redis, Cassandra and TiKV. Its listeners are neither encrypted nor
authenticated unless `server.tls`, `server.auth` and `ingester.graphite_tls`
are configured (see [Running in production](README.md#running-in-production)).
That an unconfigured deployment accepts writes and queries from anyone who
can reach it is documented behaviour, not a vulnerability.

Examples of what is in scope:

- bypassing `server.auth` or the TLS client certificate checks;
- remote write, Graphite or query requests that crash a role, or make it use
  far more memory or CPU than the documented limits allow
  (`ingester.max_*`, `querier.*`);
- data loss or corruption caused by crafted input, or by data read back from
  Redis, Cassandra or TiKV;
- secrets leaking into logs, metrics or `xtsdb config` output.

## Supported versions

Security fixes are made on the `master` branch and released in the next
version. Only the latest release is supported.

## Reporting a vulnerability

Please do **not** open a public issue, pull request or discussion.

Report the vulnerability privately with GitHub's
[private vulnerability reporting](https://github.com/yuuki/xtsdb/security/advisories/new)
(the "Report a vulnerability" button of the Security tab). If that is not
possible, contact the author privately via the contact information on
https://github.com/yuuki.

Please include the xtsdb version (`xtsdb version`), the role and the relevant
configuration (with secrets removed), the backends and their versions, what
an attacker controls (a remote write or Graphite client, a query client, a
backend, ...), a reproduction if you have one, and the impact. You should
receive an answer within a few weeks; xtsdb is maintained on a best-effort
basis.
