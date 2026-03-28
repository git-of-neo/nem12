## Initial Impressions
1. Reminds me of the "1 Billion Row Challenge" from a few years back.
2. Setting aside low-level tricks like SIMD, memory swapping, and other hacks, some kind of buffered I/O should be a good baseline; let's go with that for this.
3. The requirements specify it will be used to "handle files of very large sizes," so let's pick a language that isn't too slow (crossing out Python...). Rust would be a performant, fun experiment, but I'm going to stick with Golang since it's performant enough and I am familiar with it.

## General Idea
1. Code a parser for NEM12.
    1. Since I am not familiar with this specification, I will code a parser that's optimized for ease-of-understanding first.
    2. The idea is to parse each row into a specific record type:
        - Header record (100)
        - NMI data details record (200)
        - Interval data record (300)
        - Interval event record (400)
        - B2B details record (500)
        - End of data (900)
2. Use `bufio` to load data and parse the rows serially.

## Testing Plan
1. Use `go:embed` for the provided sample data (named `sample.NEM12`) and test it against the four expectations provided in the document:
    - Multiple `300 records` belong to the `200 record`. Of interest for this specific task are the NMI (the second value in the `200 record` — `NEM1201009` in this example).
    - The interval length (the ninth value in the `200 record` — `30` in this example).
    - The interval date (the second value in the `300 record` — e.g., `20050301`).
    - The interval values, which we call consumption (values 3-50 in the `300 record` — e.g., `0.461`).