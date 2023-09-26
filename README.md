# Command-line tools for FFS

See also https://github.com/creachadair/ffs.

- The [`blobd`](https://github.com/creachadair/ffstools/tree/main/blobd)
  tool defines an RPC service that implements the FFS blob store interface
  over various underlying key-value storage implementations.

  ```sh
  # To install:
  go install github.com/creachadair/ffstools/blobd@latest
  ```

- The [`ffs`](https://github.com/creachadair/ffstools/tree/main/ffs) tool
  also communicates with the `blobd` service and provides commands to
  manipulate the contents of the store as FFS specific messages.

  ```sh
  # To install:
  go install github.com/creachadair/ffstools/ffs@latest
  ```

  The `ffs blob` subcommand replaced the separate `blob` tool in #30.

- The [`file2json`](https://github.com/creachadair/ffstools/tree/main/file2json)
  tool decodes wire-format node messages and translates them to JSON for easier
  reading by humans.

  ```sh
  # To install:
  go install github.com/creachadair/ffstools/file2json@latest
  ```

## Installation and Usage

Install `blobd` and `ffs` as noted above, then:

```bash
# Start up a storage server (blobd) using local files as storage.
export FFS_STORE=/tmp/test.db.sock
blobd -store file:test.db -listen "$FFS_STORE" &
while [[ ! -e "$FFS_STORE" ]] ; do sleep 1 ; done

# Create a root pointer to anchor some data.
ffs root create example --desc 'Example root pointer'

# Create some files to put into storage.
mkdir -p files/sub
echo "This is my file." > files/sub/f1.txt
echo "Many others are like it" > files/sub/f2.txt
echo "But this one is mine." > files/f3.txt

# Copy the files directory into the store.
ffs put -into example/test1 files

# List the contents we just wrote...
ffs file list -long example/test1
ffs file list -long example/test1/sub

# Move some files around...
echo "That was your file." > files/sub/f1.txt
mv files/sub/f2.txt files/f4.txt
rm files/f3.txt

# Add another copy of the structure.
ffs put -into example/test2 files

# List the revised contents...
ffs file list -long example/test1
ffs file list -long example/test2
ffs file list -long example/test2/sub

# List the stuff reachable from the root.
ffs file list -long -key example

# GC unreachable data in the store.
ffs gc

# Stop the blobd.
kill %1 && wait
```
