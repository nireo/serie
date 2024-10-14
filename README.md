# serie

A embeddable database for time series data.

## Project structure

Each file contains their own tests and each file also contains a high-level overview of the code.

- `http.go` exposes a http interface for the data engine.
- `engine` implements a LSM-tree database modified to better fit time series data.
- `query.go` implements a lexer and custom query language that can be used to query time series data.