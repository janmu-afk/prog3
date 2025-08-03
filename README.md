# Programming III - Transaction Network Analysis
This is my implementation of transaction network analysis in singlethreaded, multithreaded and distributed modes of operation for the Programming III course taught at UP FAMNIT.

Compilation done for Java 21 on Windows 10, certain environment variables/syntax may differ depending on shell/OS.
## Usage
It's presumed that you've installed MPJ Express v0.44 or higher, exported it under the `MPJ_HOME` environment variable and appended `%MPJ_HOME%\bin` to the PATH environment variable. It's also presumed that your data is stored in a `data` folder within the project folder

Compilation:
` javac -cp "$env:MPJ_HOME\lib\mpj.jar" *.java utils\*.java`

Running:
` java -cp ".;%MPJ_HOME%\lib\mpj.jar" Entry x y`,
where `y` denotes the depth of graph traversal and `x` denotes:
- `1` - single-threaded mode,
- `2` - multi-threaded mode and
- `3` - distributed (multicore) mode.

### Jan Mušič, 89231245