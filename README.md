# schemer #

Welcome to schemer! Here, I am trying some schema related tricks.

## tests

## generating classes from Avro /.avsc

Sbt Avro Hugger is responsible for this task. It is integrated into the build, so calling `compile` will also generate the case classes. 

Otherwise, call `avroScalaGenerateSpecific`

## TODOs

* try kafka-serialization wrappers

https://github.com/ovotech/kafka-serialization

* try Avro IDL

https://avro.apache.org/docs/current/idl.html

* custom deserializer, wrapping GenericRecord

## Contribution policy ##

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license
the work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you
agree to license the material under the project's open source license and warrant that you have the
legal authority to do so.

## License ##

This code is open source software licensed under the
[Apache-2.0](http://www.apache.org/licenses/LICENSE-2.0) license.
