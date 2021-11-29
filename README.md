# Command-line tools for FFS

- The [`blobd`](https://github.com/creachadair/ffstools/tree/default/blobd)
  tool defines a JSON-RPC service that implements the FFS blob store interface
  over various underlying key-value storage implementations.

  ```sh
  # To install:
  go install github.com/creachadair/ffstools/blobd@latest
  ```

- The [`blob`](https://github.com/creachadair/ffstools/tree/default/blob) tool
  is a client that communicates with the `blobd` service to manipulate the
  contents of a blob store as opaque data.

  ```sh
  # To install:
  go install github.com/creachadair/ffstools/blob@latest
  ```

- The [`ffs`](https://github.com/creachadair/ffstools/tree/default/ffs) tool
  also communicates with the `blobd` service and provides commands to
  manipulate the contents of the store as FFS specific messages.

  ```sh
  # To install:
  go install github.com/creachadair/ffstools/ffs@latest
  ```

- The [`file2json`](https://github.com/creachadair/ffstools/tree/default/file2json)
  tool decodes wire-format node messages and translates them to JSON for easier
  reading by humans.

  ```sh
  # To install:
  go install github.com/creachadair/ffstools/file2json@latest
  ```
