## Potential optimizations
1. research optimisations/profile hot paths for optimisations to improve the program performance (depends on what we're optimizing for, speed, memory etc...) then run `go test -bench` on few implementations and optimize for our specific requirements (memory/speed/etc)
2. research more optimal parsing techniques for NEM12 / better ways to deal with the NEM12 data in general
3. add a load test (write a NEM12 mock data generator function, have it generate bunch of rows then benchmark the programme on the mock data)
4. right now the programme is serial, given the heavy DISK I/O nature (read from file and write to database) of the programme, probably a good idea to add some parallelism to optimize the programme
    1. My thoughts while implementing the whole thing is maybe there is a way to abuse the timing between both DISK I/O...
    2. But also memory management might be tricky....
5. database writing seems like a major bottle neck here as well, especially since we're writing into a relation databse, would explore how to optimize that
