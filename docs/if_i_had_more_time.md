## Potential Optimizations
1. Research and profile hot paths to improve performance (depending on whether we're optimizing for speed, memory, etc.). I would then run `go test -bench` on several implementations and optimize for specific requirements.
2. Investigate more efficient parsing techniques for NEM12 and better ways to handle NEM12 data in general.
3. Add a load test: write a NEM12 mock data generator, have it generate a large number of rows, and then benchmark the program using that data.
4. Currently, the program is serial. Given the heavy disk I/O nature (reading from files and writing to the database), adding parallelism would likely optimize performance.
    1. I suspect there is a way to leverage the timing between both disk I/O operations.
    2. However, memory management might become tricky.
5. Database writing seems to be a major bottleneck, especially since we are writing to a relational database; I would explore how to optimize those transactions.
6. Conduct a more thorough code review of the `main.go` structure. The initial focus was on ensuring correct output and iterating with Sonnet to generate code that met my requirements due to time constraints.
7. Make the streaming parser configurable and benchmark the best settings (should probably try to align it with the `bufio` buffer size).