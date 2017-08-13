# fwish

Simple SQL database migration tool and package for Go programming language.
It's inspired and roughly compatible with Flyway, or you can say that it's
Flyway-ish, fw-ish.

## Project Status

The project is still in very early development. It's barely functional
and it's not fully compatible with Flyway yet. The stability is not
guaranteed, interfaces will change, etc. So you know that you should not
use this if you care about your data(bases).

### Milestone 1

The goal for milestone 1 is that fwish these features:

  - Full compatibility with Flyway-migrated database. People
    could use both simultaneously and there should be no problems.
  - Full compatibility with Flyway's SQL file-based migration source.
  - Support for schema identifier verifications.
  - Extensive test cases.
