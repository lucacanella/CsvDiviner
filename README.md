# CsvDiviner

## What's this
A tool that analyzes csv files and extracts information about column types and features.
It's proven useful analyzing quite heavy CSV files (up to 18Gb in less than 5 minutes) giving information to create correct database tables.

## How to
Compile and run with the following command:
```bash
java CsvDiviner.jar input_file_path -b 4000 -w 7 -l WARNING -o output_file_path
```

Other parameters and usage:
```
Usage: java --jar csv-diviner.jar [options] input_file
  Options:
    -b, --batch-size
      Batch size - how many rows must be read before being sent to worker 
      thread. 
      Default: 10000
    -c, --charset
      Set input file encoding (defaults to UTF-8, see 
      https://docs.oracle.com/javase/8/docs/technotes/guides/intl/encoding.doc.html). 
      Default: UTF-8
    -e, --escapechar
      Csv escape char - defaults to '\'.
      Default: \
    -h, --help-usage
      Print help.
      Default: false
    -t, --line-terminator
      Sets the line terminator (options are 'LF' or 'CRLF').
    -l, --logger
      Set logger level (OFF|INFO|WARNING|ERROR).
      Default: ERROR
    -o, --output
      If set, writes analysis json result in a file at the given path.
    --printStats
      Print execution stats
      Default: false
    -q, --quotechar
      Csv fields quote char - defaults to '"'.
      Default: "
    -s, --separator
      Csv fields separator, defaults to ','.
      Default: ,
    --showProgress
      Print execution stats
      Default: false
    --silent
      Sets silent mode (prints only logs).
      Default: false
    -v, --verbose
      Prints some information.
      Default: false
    --version
      Prints version information.
      Default: false
    -w, --worker-threads
      Number of worker threads.
      Default: 7
```

## Compile

Compile by running the "assembly:assembly" goal (`mvn assembly:assembly`).
