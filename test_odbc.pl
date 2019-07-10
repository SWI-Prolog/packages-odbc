/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2015, University of Amsterdam
                         VU University Amsterdam
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

:- module(test_odbc,
          [ test_odbc/0,
            test_odbc/1,                % +ConnectionString
            test_odbc/2                 % +DSN, +Options
          ]).
                                        % use the local copy
:- asserta(user:file_search_path(foreign, '.')).
:- asserta(user:file_search_path(library, '.')).
:- asserta(user:file_search_path(library, '../plunit')).

:- use_module(library(odbc)).
:- use_module(library(lists)).
:- use_module(library(apply)).
:- use_module(library(plunit)).

/** <module> Test ODBC Interface

This module tests the ODBC interface. The   default  test is designed to
run with SQLite version 2,  installed   using  the  driver =SQLite=. The
versions test_odbc/1 and test_odbc/2  allow   connecting  to alternative
databases.

@tbd    The current version of this test file does not return failure.
        Failure will be enabled when the tests are more mature.
*/

%!  test_odbc is semidet.
%!  test_odbc(+ConnectString) is semidet.
%!  test_odbc(+DSN, +Options) is semidet.
%
%   Run ODBC tests.  Without  options,  this   tries  to  create  an
%   anonymous  memory  based  ODBC  connection  using  SQLite.  This
%   requires the SQLite driver  to  be   configured  on  the hosting
%   machine. With options, odbc_connect/3 is called with the DSN and
%   options.  The caller must ensure ODBC is properly installed.

run_odbc_tests :-
    \+ getenv('USE_ODBC_TESTS', false).

test_odbc :-
    (   run_odbc_tests
    ->  test_odbc('DRIVER=SQLite3;Database=test.sqlite')
    ;   true
    ).
test_odbc(ConnectString) :-
    delete_db_file(ConnectString),
    catch(open_db(ConnectString), E, print_message(error, E)),
    (   var(E)
    ->  run_tests([ odbc
                  ])
    ;   true
    ),
    delete_db_file(ConnectString).
test_odbc(DSN, Options) :-
    catch(open_db(DSN-Options), E, print_message(error, E)),
    (   var(E)
    ->  run_tests([ odbc
                  ])
    ;   true
    ).

:- dynamic
    params/1.

open_db(DSN-Options) :-
    !,
    asserta(params(DSN-Options)),
    odbc_connect(DSN, _,
                 [ alias(test),
                   open(once)
                 | Options
                 ]).
open_db(ConnectString) :-
    asserta(params(ConnectString)),
    odbc_driver_connect(ConnectString,
                        _Connection,
                        [ open(once),
                          alias(test)
                        ]).

open_db :-
    params(Params),
    !,
    open_db(Params).

delete_db_file(ConnectString) :-
    split_string(ConnectString, ";", " ", Parts),
    member(Part, Parts),
    split_string(Part, "=", "", ["Database",File]),
    !,
    catch(delete_file(File), _, true).
delete_db_file(_).


:- begin_tests(odbc).

test(integer)       :- test_type(integer).
test(integer_float) :- test_type(integer - 'Type integer: as float').
test(integer_atom)  :- test_type(integer - 'Type integer: as atom').
test(bigint)        :- test_type(bigint).
test(float)         :- test_type(float).
test(decimal_10_2)  :- test_type(decimal(10,2)).
test(decimal_6_2)   :- test_type(decimal(6,2)).
test(decimal_14_2)  :- test_type(decimal(14,2)).
test(numeric_10)    :- test_type(numeric(10)).
test(varchar_20)    :- test_type(varchar(20)).
test(varchar_10)    :- test_type(varchar(10)).
test(varchar_100)   :- test_type(varchar(100)).
test(varchar_2000)  :- test_type(varchar(2000)).
test(varbinary_20)  :- test_type(varbinary(20)).
test(blob)          :- test_type(blob).
test(longblob)      :- test_type(longblob).
test(date)          :- test_type(date - 'Type date').
test(date_null)     :- test_type(date - 'Type date: NULL').
test(time)          :- test_type(time - 'Type time').
test(time_null)     :- test_type(time - 'Type time: NULL').
test(timestamp)     :- test_type(timestamp).
test(columns)       :- tcolumns.

test(cursor,
     [ setup(make_mark_table),
       all(Name1-Name2 == [john-john, bob-bob, bob-mary,
                           chris-chris, mary-bob, mary-mary])
     ]) :-
    same_mark(Name1, Name2).

:- end_tests(odbc).

                 /*******************************
                 *           TYPE TESTS         *
                 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Test the type-conversion system. For each type   we create a small table
and read it back. If the type   can  be accessed with alternative Prolog
types we test this too.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

%!  type(?SqlType, ?PlTypeAndValues, ?AltTypeMap, ?Options)
%
%   Define a test-set.  The first argument is the SQL type to test.
%   PlType is the default Prolog type with a set of values.  AltType
%   is an alternative type that can be used to exchange values and
%   Map is a maplist/3 argument to convert the value-list into a
%   list of values of the alternative type.
%
%   SQLite issues:
%
%     - -1, when read as a float is 4294967295.0
%     - Writing integers as text does not work.
%     - Decimal tests strip trailing 0s

type(integer,
     integer = [-1, 0, 42, '$null$' ],
     [
     ],
     []).
type(integer - 'Type integer: as float',
     integer = [-1, 0, 42, '$null$' ],
     [ float = integer_to_float         % exchange as float
     ],
     [ \+ dbms_name('SQLite') ]).
type(integer - 'Type integer: as atom',
     integer = [-1, 0, 42, '$null$' ],
     [ atom  = integer_to_atom          % exchange as text
     ],
     [ \+ dbms_name('SQLite') ]).
type(bigint,
     integer = [-1, 0, 42, 35184372088832, -35184372088832, '$null$' ],
     [ atom  = integer_to_atom,         % exchange as text
       float = integer_to_float         % exchange as float
     ],
     [ \+ dbms_name('SQLite') ]).
type(float,
     float   = [-1.0, 0.0, 42.0, 3.2747, '$null$' ],
     [
     ],
     []).
type(decimal(10,2),
     atom = ['3.43', '4.55', '5.05', '$null$'],
     [
     ],
     []).
type(decimal(6,2),                      % truncating test
     atom = ['1000.01'],
     [
     ],
     []).
type(decimal(14,2),                     % truncating test
     atom = ['17.45'],
     [
     ],
     []).
type(numeric(10),
     integer = [-1, 0, 42, '$null$'],
     [
     ],
     []).
type(varchar(20),
     atom = [ 'foo',
              '',
              'this is a long text',
              '$null$'
            ],
     [ codes   = sql_atom_codes,
       string  = atom_to_string
     ],
     []).
type(varchar(10),                               % can we access as integers?
     atom = [ '1', '2', '$null$'
            ],
     [ integer = atom_to_integer
     ],
     []).
type(varchar(100),
     atom = [ foo,
              '',
              'This is a nice long string consisting of enough text',
              '$null$'
            ],
     [ codes   = sql_atom_codes
     ],
     []).
type(varchar(2000),                             % can we access as integers?
     atom = [ 'This is a nice atom',
              '',
              Long,
              '$null$'
            ],
     [
     ],
     [ \+ dbms_name('MySQL')
     ]) :-
    findall(C, (between(1, 1500, X),
                C is X mod 64 + "@"), LongCodes),
    atom_chars(Long, LongCodes).
type(varbinary(20),
     atom = [ foo,
              '',
              'With a \0\ character'
            ],
     [
     ],
     [ \+ dbms_name('MySQL'),
       \+ dbms_name('SQLite')
     ]).
type(blob,                      % mySql blob
     atom = [ foo,
              '',
              'With a \0\ character'
            ],
     [
     ],
     [ odbc_type(longvarbinary),
       dbms_name('MySQL')               % MySQL specific test
     ]).
type(longblob,                  % mySql blob
     atom = [ BIG
            ],
     [
     ],
     [ odbc_type(longvarbinary),
       dbms_name('MySQL')               % MySQL specific test
     ]) :-
    length(Bytes, 10 000),
    maplist(random_between(0, 255), Bytes),
    atom_codes(BIG, Bytes).
type(date - 'Type date',
     date = [ date(1960,3,19) ],
     [
     ],
     [ \+ dbms_name('Microsoft SQL Server')
     ]).
type(date - 'Type date: NULL',
     date = [ '$null$' ],
     [
     ],
     [ \+ dbms_name('Microsoft SQL Server'),
       \+ dbms_name('MySQL')
     ]).
type(time - 'Type time',
     time = [ time(18,23,19) ],
     [
     ],
     [ \+ dbms_name('Microsoft SQL Server')
     ]).
type(time - 'Type time: NULL',
     time = [ '$null$' ],
     [
     ],
     [ \+ dbms_name('Microsoft SQL Server'),
       \+ dbms_name('MySQL')
     ]).
type(timestamp,                         % MySQL uses POSIX stamps
     timestamp = [ timestamp(1990,5,18,18,23,19,0) ],
     [ integer = timestamp_to_integer
     ],
     [ dbms_name('MySQL')
     ]).

%!  test_type(+TypeSpec)
%
%   Test round-trip conversion for TypeSpec.

test_type(TypeSpec) :-
    open_db,
    once(type(TypeSpec, PlType=Values, AltAccess, Options)),
    db_type(TypeSpec, Type, Comment),
    (   applicable(Options, Type)
    ->  progress('~w:', [Comment]),
        create_test_table(Type),
        progress(' (w)', []),
        (   memberchk(odbc_type(ODBCType), Options)
        ->  true
        ;   ODBCType = Type
        ),
        insert_values(test, ODBCType, PlType, Values),
        progress(' (r)', []),
        read_values(test, PlType, ReadValues),
        compare_sets(Values, ReadValues),
        read_test_alt_types(AltAccess, test, Values),
        write_test_alt_types(AltAccess, Type, test),
        progress(' (OK!)~n', [])
    ;   true
        % progress('Skipped ~w: not supported~n', [Comment])
    ).

db_type(Type-Comment, Type, Comment) :- !.
db_type(Type,         Type, Comment) :-
    format(string(Comment), 'Type ~w', [Type]).

%!  applicable(+Options, +Type)
%
%   See whether we can run this test on this connection.

applicable([], _) :- !.
applicable([H|T], Type) :-
    !,
    applicable(H, Type),
    applicable(T, Type).
applicable(\+ Option, Type) :-
    !,
    \+ applicable(Option, Type).
applicable((A;B), Type) :-
    !,
    (   applicable(A, Type)
    ->  true
    ;   applicable(B, Type)
    ).
applicable(dbms_name(DB), _) :-
    !,
    odbc_get_connection(test, dbms_name(DB)).
applicable(_, _).


%!  read_test_alt_types(+TypeMaps, +Table, -Values)
%
%   Try to read the table using alternative Prolog types and check
%   the results.

read_test_alt_types([], _, _).
read_test_alt_types([Type=Map|T], Table, Values) :-
    read_test_alt_type(Type, Map, Table, Values),
    read_test_alt_types(T, Table, Values).

read_test_alt_type(Type, Map, Table, Values) :-
    progress(' r(~w)', Type),
    maplist(Map, Values, AltValues),
    read_values(Table, Type, ReadValues),
    compare_sets(AltValues, ReadValues).

%!  write_test_alt_types(+TypeMaps, +Table, +Values)

write_test_alt_types([], _, _).
write_test_alt_types([Type=Map|T], SqlType, Table) :-
    write_test_alt_type(Type, Map, SqlType, Table),
    write_test_alt_types(T, SqlType, Table).

write_test_alt_type(Type, Map, SqlType, Table) :-
    progress(' w(~w)', Type),
    once(type(SqlType, PlType=NativeValues, _, _)),
    odbc_query(test, 'delete from ~w'-[Table]),
    maplist(Map, NativeValues, Values),
    insert_values(test, SqlType, Type, Values),
    read_values(test, PlType, ReadValues),
    compare_sets(NativeValues, ReadValues).

%!  insert_values(+Table, +OdbcType, +PlType, +Values)
%
%   Insert Prolog values into the table

insert_values(Table, SqlType, PlType, Values) :-
    open_db,
    odbc_prepare(test,
                 'insert into ~w (testval) values (?)'-[Table],
                 [ PlType>SqlType ],
                 Statement),
    forall(member(V, Values),
           odbc_execute(Statement, [V])),
    odbc_free_statement(Statement).

read_values(Table, PlType, Values) :-
    open_db,
    findall(Value,
            odbc_query(test,
                       'select (testval) from ~w'-[Table],
                       row(Value),
                       [ types([PlType])
                       ]),
            Values).

%!  compare_sets(+Expected, +Read) is det.
%
%   Compare two sets of values and print the differences.

compare_sets(X, X) :- !.
compare_sets(X, Y) :-
    compare_elements(X, Y).

compare_elements([], []).
compare_elements([H|T0], [H|T1]) :-
    !,
    compare_elements(T0, T1).
compare_elements([H0|T0], [H1|T1]) :-
    (   float(H0),
        float(H1)
    ->  Diff is H0-H1,
        format('~NERROR: ~q != ~q (diff=~f)~n', [H0, H1, Diff])
    ;   print_val(H0, P0),
        print_val(H1, P1),
        format('~NERROR: ~q != ~q~n', [P0, P1])
    ),
    compare_elements(T0, T1).


print_val(H, P) :-
    atom(H),
    atom_length(H, Len),
    Len > 60,
    !,
    sub_atom(H, 0, 40, _, Start),
    sub_atom(H, _, 20, 0, End),
    atomic_list_concat([Start, '...', End], P).
print_val(H, H).

                 /*******************************
                 *             MAPS             *
                 *******************************/

integer_to_atom('$null$', '$null$') :- !.
integer_to_atom(Int, Atom) :-
    (   number(Int)
    ->  number_codes(Int, Codes),
        atom_codes(Atom, Codes)
    ;   atom_codes(Atom, Codes),
        number_codes(Int, Codes)
    ).

integer_to_float('$null$', '$null$') :- !.
integer_to_float(Int, Float) :-
    Float is float(Int).

atom_to_string('$null$', '$null$') :- !.
atom_to_string(Atom, String) :-
    atom_string(Atom, String).

atom_to_integer('$null$', '$null$') :- !.
atom_to_integer(Atom, Int) :-
    integer_to_atom(Int, Atom).

sql_atom_codes('$null$', '$null$') :- !.
sql_atom_codes(Atom, Codes) :-
    atom_codes(Atom, Codes).

timestamp_to_integer('$null$', '$null$') :- !.
timestamp_to_integer(timestamp(Y,M,D,H,Mn,S,0), Sec) :-
    date_time_stamp(date(S,Mn,H,D,M,Y,_UTCOff,_TZ,_DST), Sec).


                 /*******************************
                 *          SHOW SOURCE         *
                 *******************************/

list(Name) :-
    open_db,
    (   odbc_query(test, 'select * from ~w'-[Name], Row, [source(true)]),
        writeln(Row),
        fail
    ;   true
    ).

                 /*******************************
                 *          CURSOR TEST         *
                 *******************************/

:- dynamic
    statement/2.

delete_statements :-
    forall(retract(statement(_, Statement)),
           odbc_free_statement(Statement)).


mark(john,  6).
mark(bob,   7).
mark(chris, 5).
mark(mary,  7).

make_mark_table :-
    open_db,
    delete_statements,
    catch(odbc_query(test, 'drop table marks'), _, true),
    odbc_query(test, 'create table marks (name char(25), mark integer)'),
    odbc_prepare(test,
                 'insert into marks (name,mark) values (?,?)',
                 [ char(25),
                   integer
                 ],
                 Statement),
    forall(mark(Name, Mark),
           odbc_execute(Statement, [Name,Mark])),
    odbc_free_statement(Statement).

db_mark(Name, Mark) :-
    (   statement(select_mark, Stmt)
    ->  true
    ;   odbc_prepare(test, 'select * from marks', [], Stmt),
        assert(statement(select_mark, Stmt))
    ),
    odbc_execute(Stmt, [], row(Name, Mark)).

same_mark(Name1, Name2) :-
    db_mark(Name1, Mark1),
    db_mark(Name2, Mark2),
    Mark1 == Mark2.

marks(L) :-
    open_db,
    odbc_query(test,
               'select * from marks', L,
               [findall(mark(X,Y), row(X,Y))]).

with_mark(Mark, L) :-                   % doesn't work yet
    open_db,
    odbc_query(test,
               'select * from marks', L,
               [ findall(Name, row(Name, Mark))
               ]).

tmark :-
    open_db,
    odbc_query(test, 'SELECT * from marks', row(X, 6)),
    write(X),
    fail.

prepfoo :-
    open_db,
    odbc_prepare(test,
                 'select name from foo where mark=?',
                 [default],
                 S,
                 []),
    writeln(S).


                 /*******************************
                 *       GENERIC ACTIONS        *
                 *******************************/

create_test_table(Type) :-
    catch(odbc_query(test, 'drop table test'), _, true),
    odbc_query(test,
               'create table ~w (testval ~w)'-[test, Type]).


                 /*******************************
                 *         SPECIAL TESTS        *
                 *******************************/

test(decimal) :-
    create_test_table(decimal(14,2)),
    odbc_prepare(test,
                 'insert into test (testval) values (?)',
                 [ decimal(14,2) ],
                 Statement),
    odbc_execute(Statement, ['17.45'], affected(Affected)),
    odbc_free_statement(Statement),
    progress('Affected ~w rows', [Affected]),
    odbc_query(test, 'select * from test', row('17.45')),
    progress(' OK!~n', []).


                 /*******************************
                 *             FETCH            *
                 *******************************/

create_fetch_table :-
    open_db,
    create_test_table(integer),
    odbc_prepare(test,
                 'insert into test (testval) values (?)',
                 [ integer ],
                 Statement),
    forall(between(1, 100, X),
           odbc_execute(Statement, [X])),
    odbc_free_statement(Statement).

fetch(Options) :-
    open_db,
    odbc_set_connection(test, cursor_type(static)),
    odbc_prepare(test,
                 'select (testval) from test',
                 [],
                 Statement,
                 [ fetch(fetch)
                 ]),
    odbc_execute(Statement, []),
    fetch(Statement, Options).

fetch(Statement, Options) :-
    odbc_fetch(Statement, Row, Options),
    (   Row == end_of_file
    ->  true
    ;   writeln(Row),
        fetch(Statement, Options)
    ).


                 /*******************************
                 *             META             *
                 *******************************/

tcolumns :-
    open_db,
    make_mark_table,
    findall(X, odbc_table_column(test, marks, X), List),
    List = [name, mark].


                 /*******************************
                 *           FEEDBACK           *
                 *******************************/

progress(_, _) :-
    current_prolog_flag(verbose, silent),
    !.
progress(Fmt, Args) :-
    format(Fmt, Args),
    flush_output.


                 /*******************************
                 *             PORTRAY          *
                 *******************************/

user:portray(Long) :-
    atom(Long),
    atom_length(Long, Len),
    Len > 70,
    sub_atom(Long, 0, 20, _, Start),
    sub_atom(Long, _, 10, 0, End),
    format('\'~w...~w\'', [Start, End]).

                 /*******************************
                 *    Primary & foreign keys    *
                 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
this interface has been developed on purpose to implement
W3C 'direct mapping of RDB to RDF'. The database definition is then the same
as documented in http://www.w3.org/TR/rdb-direct-mapping/.
If tested with MySQL, then tables must use InnoDB as storage engine,
that allows for foreign keys definition. Here is the dump required to
rebuild the test case. Database is named rdb2rdf_dm

-- Adminer 3.6.1 MySQL dump

SET NAMES utf8;
SET foreign_key_checks = 0;
SET time_zone = 'SYSTEM';
SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';

DROP TABLE IF EXISTS `Addresses`;
CREATE TABLE `Addresses` (
  `ID` int(11) NOT NULL DEFAULT '0',
  `city` char(10) NOT NULL,
  `state` char(2) NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

INSERT INTO `Addresses` (`ID`, `city`, `state`) VALUES
(18,    'Cambridge',    'MA');

DROP TABLE IF EXISTS `People`;
CREATE TABLE `People` (
  `ID` int(11) NOT NULL,
  `fname` char(10) NOT NULL,
  `addr` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  KEY `addr` (`addr`),
  CONSTRAINT `People_ibfk_2` FOREIGN KEY (`addr`) REFERENCES `Addresses` (`ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

INSERT INTO `People` (`ID`, `fname`, `addr`) VALUES
(7,     'Bob',  18),
(8,     'Sue',  NULL);

-- 2013-01-03 09:05:38

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

test_keys_connect(Cn) :-
    ( passwd(Pwd) -> true ; getpass(Pwd), assert(passwd(Pwd)) ),
    Db = rdb2rdf_dm,
    Uid = root,  % normally this is user name
    format(atom(S), 'driver=mysql;db=~s;uid=~s;pwd=~s', [Db, Uid, Pwd]),
    odbc_driver_connect(S, Cn, []).

test_pk_keys :-
    test_keys_connect(Cn),
    forall(odbc_table_primary_key(Cn, T, K),
           writeln(primary_key(T, K))).

test_fk_keys :-
    test_keys_connect(Cn),
    forall((
            Tpk = 'Addresses',
            odbc_table_foreign_key(Cn, Tpk, Cpk, Tfk, Cfk),
            writeln(foreign_key(Tpk, Cpk, Tfk, Cfk))
        ;
            Tfk = 'People',
            odbc_table_foreign_key(Cn, Tpk, Cpk, Tfk, Cfk),
            writeln(foreign_key(Tpk, Cpk, Tfk, Cfk))
        ), true).
