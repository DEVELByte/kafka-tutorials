## AVRO
Avro is a remote procedure call and data serialization framework. It was developed by
Doug Cutting, the father of Hadoop. Since Hadoop writable classes lack
language portability, Avro becomes quite helpful, as it deals with data
formats that can be processed by multiple languages. Avro is a preferred
tool to serialize data in Hadoop.


##### Features of Avro:
- language-neutral data serialization system
- Rich data structures.
- A compact, fast, binary data format.
- A container file, to store persistent data.
- Remote procedure call (RPC).
- Simple integration with dynamic languages. Code generation is not required to read or write data files nor to use or implement RPC protocols. Code generation as an optional optimization, only worth implementing for statically typed languages.


A key feature of Avro is robust support for data schemas that change over time - often called schema evolution. Avro cleanly handles schema changes like missing fields, added fields and changed fields; as a result, old programs can read new data and new programs can read old data

Avro read and write operations are associated with schema, Avro schemas are defined with with JSON . This facilitates implementation in languages that already have JSON libraries. When Avro data is stored in a file, its schema is stored with it, so that files may be processed later by any program. If the program reading the data expects a different schema this can be easily resolved, since both schemas are present.
When Avro is used in RPC, the client and server exchange schemas in the connection handshake.Since both client and server both have the other's full schema, correspondence between same named fields, missing fields, extra fields, etc. can all be easily resolved.


<strong>An Avro Object Container File consists of:</strong>

- A file header, followed by
- one or more file data blocks.

<strong>A file header consists of:</strong>

- Four bytes, ASCII 'O', 'b', 'j', followed by 1.
- file metadata, including the schema definition.
- The 16-byte, randomly-generated sync marker for this file.

For data blocks Avro specifies two serialization encodings: <strong>binary</strong> and <strong>JSON</strong>. Most applications will use the binary encoding, as it is smaller and faster. For debugging and web-based applications, the JSON encoding may sometimes be appropriate.

For More Info on How to work with avro in python
http://avro.apache.org/docs/current/gettingstartedpython.html
