## Initial Impressions
1. This reminds me of the "1 Billion Row Challenge" from a few years back.
2. Setting aside low-level tricks like SIMD and memory swapping, buffered I/O provides a solid baseline; I'll use that for this implementation.
3. The requirements specify the tool will "handle files of very large sizes," so I chose a performant language. While Rust would be a fun experiment, I’m sticking with Go as it is sufficiently performant and I am familiar with it.
4. Since the instructions mention "does not include any form of infrastructure and deployment code," I’ll use SQLite. It is the easiest database to spin up without external infrastructure.

## General Idea
1. Implement a parser for NEM12.
    1. As I am not familiar with this specification, I will prioritize ease of understanding for the first iteration.
    2. The parser will map each row to a specific record type:
        - Header record (100)
        - NMI data details record (200)
        - Interval data record (300)
        - Interval event record (400)
        - B2B details record (500)
        - End of data (900)
2. Use `bufio` to load data and parse rows serially.
3. Spawn goroutines to handle SQLite `INSERT` operations. I'll use a worker pool with a configurable constant to control the pool size, ensuring the database isn't overloaded by concurrent writes.
4. Use a persistent, local SQLite database to verify the final output.

## Testing Plan
1. Use `go:embed` for the provided sample data (`sample.NEM12`) and test it against the four expectations provided in the requirements:
    - Multiple `300 records` belong to a `200 record`. Of interest is the NMI (the second value in the `200 record` — `NEM1201009`).
    - The interval length (the ninth value in the `200 record` — `30`).
    - The interval date (the second value in the `300 record` — e.g., `20050301`).
    - The interval values (consumption) (values 3-50 in the `300 record` — e.g., `0.461`).
2. Use an in-memory SQLite database to test database writing functions.

## Manual Testing & Sanity Checks
1. Print parsed output to `stdout`.
2. Run `SELECT` statements against the SQLite database after execution to verify the data is present and correct.