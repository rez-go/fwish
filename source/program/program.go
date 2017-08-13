package program

//TODO: use other program to provide the migrations. the idea is
// we define some protocol, or we can find existing established
// one. we will execute the program with the appropriate commands
// to get the required functionality.
//
// Note that the program itself should not manage the meta.
//
// for example:
//
//     $ program schema-id
//     $ program migrations
//     $ program execute <migration>
