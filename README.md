# jepsen.rqlite

Jepsen tests for the [rqlite](https://github.com/rqlite/rqlite) database.

## Usage

Running the tests requires that you have [Vagrant](https://www.vagrantup.com/) to start the virtual machines, and
Clojure's package manager [leiningen](https://leiningen.org/) to build and run the actual test code.

Once you have all the necessary dependencies installed, running the tests is as simple as:

```shell
./run.sh
```

This will start the virtual machines for you and invoke leiningen to run the tests. You can refer
to `src/jepsen/rqlite.clj` for command-line options such as the `--nemesis-type` flag to select the nemesis used. They can be passed directly to the script:

```shell
./run.sh --nemesis-type pause
```

For more details, check out our [blog post](https://github.com/wildarch/jepsen.rqlite/blob/main/doc/blog.md).