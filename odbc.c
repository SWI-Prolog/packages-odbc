/*  $Id$

    Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        wielemak@science.uva.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 1985-2007, University of Amsterdam

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

    As a special exception, if you link this library with other files,
    compiled with a Free Software compiler, to produce an executable, this
    library does not by itself cause the resulting executable to be covered
    by the GNU General Public License. This exception does not however
    invalidate any other reasons why the executable file might be covered by
    the GNU General Public License.
*/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
This module is based on pl_odbc.{c,pl},   a  read-only ODBC interface by
Stefano  De  Giorgi  (s.degiorgi@tin.it).
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#ifdef __WINDOWS__
#include <windows.h>
#define HAVE_MKTIME 1
#define HAVE_GMTIME 1
#if (_MSC_VER >= 1400)
#define HAVE_SQLLEN 1			/* should depend on SDK version? */
#define HAVE_SQLULEN 1
#endif
#else
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#endif

#define O_DEBUG 1

static int odbc_debuglevel = 0;

#ifdef O_DEBUG
#define DEBUG(level, g) if ( odbc_debuglevel >= (level) ) g
#else
#define DEBUG(level, g) ((void)0)
#endif

#include <sql.h>
#include <sqlext.h>
#include <time.h>
#include <limits.h>			/* LONG_MAX, etc. */

#ifndef HAVE_SQLLEN
#define SQLLEN DWORD
#endif
#ifndef HAVE_SQLULEN
#define SQLULEN SQLUINTEGER
#endif

#ifndef SQL_COPT_SS_MARS_ENABLED
#define SQL_COPT_SS_MARS_ENABLED 1224
#endif

#ifndef SQL_MARS_ENABLED_YES
#define SQL_MARS_ENABLED_YES (SQLPOINTER)1
#endif

#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#ifndef NULL
#define NULL 0
#endif
#define MAX_NOGETDATA 1024		/* use SQLGetData() on wider columns */
#ifndef STRICT
#define STRICT
#endif

#define NameBufferLength 256
#define CVNERR -1			/* conversion error */

#if defined(_REENTRANT) && 0
#include <pthread.h>

					/* FIXME: Actually use these */
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
#define LOCK() pthread_mutex_lock(&mutex)
#define UNLOCK() pthread_mutex_unlock(&mutex)
#else
#define LOCK()
#define UNLOCK()
#endif

#if !defined(HAVE_TIMEGM) && defined(HAVE_MKTIME)
#define EMULATE_TIMEGM
static time_t timegm(struct tm *tm);
#define HAVE_TIMEGM
#endif

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Work around bug in MS SQL Server that reports NumCols of SQLColumns()
as 19, while there are only 12.  Grrr!

This bug appears fixed now, so we'll remove the work-around
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

/*#define SQL_SERVER_BUG	1*/

static atom_t    ATOM_row;		/* "row" */
static atom_t    ATOM_informational;	/* "informational" */
static atom_t	 ATOM_default;		/* "default" */
static atom_t	 ATOM_once;		/* "once" */
static atom_t	 ATOM_multiple;		/* "multiple" */
static atom_t	 ATOM_commit;		/* "commit" */
static atom_t	 ATOM_rollback;		/* "rollback" */
static atom_t	 ATOM_atom;
static atom_t	 ATOM_string;
static atom_t	 ATOM_codes;
static atom_t	 ATOM_float;
static atom_t	 ATOM_integer;
static atom_t	 ATOM_time;
static atom_t	 ATOM_date;
static atom_t	 ATOM_timestamp;
static atom_t	 ATOM_all_types;
static atom_t	 ATOM_null;		/* default null atom */
static atom_t	 ATOM_;			/* "" */
static atom_t	 ATOM_read;
static atom_t	 ATOM_update;
static atom_t    ATOM_dynamic;
static atom_t	 ATOM_forwards_only;
static atom_t	 ATOM_keyset_driven;
static atom_t	 ATOM_static;
static atom_t	 ATOM_auto;
static atom_t	 ATOM_fetch;
static atom_t	 ATOM_end_of_file;
static atom_t	 ATOM_next;
static atom_t	 ATOM_prior;
static atom_t	 ATOM_first;
static atom_t	 ATOM_last;
static atom_t	 ATOM_absolute;
static atom_t	 ATOM_relative;
static atom_t	 ATOM_bookmark;

static functor_t FUNCTOR_timestamp7;	/* timestamp/7 */
static functor_t FUNCTOR_time3;		/* time/7 */
static functor_t FUNCTOR_date3;		/* date/3 */
static functor_t FUNCTOR_odbc3;		/* odbc(state, code, message) */
static functor_t FUNCTOR_error2;	/* error(Formal, Context) */
static functor_t FUNCTOR_type_error2;	/* type_error(Term, Expected) */
static functor_t FUNCTOR_domain_error2;	/* domain_error(Term, Expected) */
static functor_t FUNCTOR_existence_error2; /* existence_error(Term, Expected) */
static functor_t FUNCTOR_representation_error1; /* representation_error(What) */
static functor_t FUNCTOR_resource_error1; /* resource_error(Error) */
static functor_t FUNCTOR_permission_error3;
static functor_t FUNCTOR_odbc_statement1; /* $odbc_statement(Id) */
static functor_t FUNCTOR_odbc_connection1;
static functor_t FUNCTOR_user1;
static functor_t FUNCTOR_password1;
static functor_t FUNCTOR_driver_string1;
static functor_t FUNCTOR_alias1;
static functor_t FUNCTOR_mars1;
static functor_t FUNCTOR_open1;
static functor_t FUNCTOR_auto_commit1;
static functor_t FUNCTOR_types1;
static functor_t FUNCTOR_minus2;
static functor_t FUNCTOR_gt2;
static functor_t FUNCTOR_context_error3;
static functor_t FUNCTOR_data_source2;
static functor_t FUNCTOR_null1;
static functor_t FUNCTOR_source1;
static functor_t FUNCTOR_column3;
static functor_t FUNCTOR_access_mode1;
static functor_t FUNCTOR_cursor_type1;
static functor_t FUNCTOR_silent1;
static functor_t FUNCTOR_findall2;	/* findall(Term, row(...)) */
static functor_t FUNCTOR_affected1;
static functor_t FUNCTOR_fetch1;
static functor_t FUNCTOR_wide_column_threshold1;	/* set max_nogetdata */

#define SQL_PL_DEFAULT  0		/* don't change! */
#define SQL_PL_ATOM	1		/* return as atom */
#define SQL_PL_CODES	2		/* return as code-list */
#define SQL_PL_STRING	3		/* return as string */
#define SQL_PL_INTEGER	4		/* return as integer */
#define SQL_PL_FLOAT	5		/* return as float */
#define SQL_PL_TIME	6		/* return as time/3 structure */
#define SQL_PL_DATE	7		/* return as date/3 structure */
#define SQL_PL_TIMESTAMP 8		/* return as timestamp/7 structure */

#define PARAM_BUFSIZE (SQLLEN)sizeof(double)

typedef unsigned long code;

typedef struct
{ SWORD        cTypeID;			/* C type of value */
  SWORD	       plTypeID;		/* Prolog type of value */
  SWORD	       sqlTypeID;		/* Sql type of value */
  SWORD	       scale;			/* Scale */
  SQLPOINTER  *ptr_value;		/* ptr to value */
  SQLLEN       length_ind;		/* length/indicator of value */
  SQLLEN       len_value;		/* length of value (as parameter)  */
  term_t       put_data;		/* data to put there */
  struct
  { atom_t table;			/* Table name */
    atom_t column;			/* column name */
  } source;				/* origin of the data */
  char	       buf[PARAM_BUFSIZE];	/* Small buffer for simple cols */
} parameter;

typedef struct
{ enum
  { NULL_VAR,				/* represent as variable */
    NULL_ATOM,				/* some atom */
    NULL_FUNCTOR,			/* e.g. null(_) */
    NULL_RECORD				/* an arbitrary term */
  } nulltype;
  union
  { atom_t atom;			/* as atom */
    functor_t functor;			/* as functor */
    record_t record;			/* as term */
  } nullvalue;
  int references;			/* reference count */
} nulldef;				/* Prolog's representation of NULL */

typedef struct
{ int references;			/* reference count */
  unsigned flags;			/* misc flags */
  code     codes[1];			/* executable code */
} findall;

typedef struct connection
{ long	       magic;			/* magic code */
  atom_t       alias;			/* alias name of the connection */
  atom_t       dsn;			/* DSN name of the connection */
  HDBC	       hdbc;			/* ODBC handle */
  nulldef     *null;			/* Prolog null value */
  unsigned     flags;			/* general flags */
  int	       max_qualifier_lenght;	/* SQL_MAX_QUALIFIER_NAME_LEN */
  SQLULEN      max_nogetdata;		/* handle as long field if larger */
  struct connection *next;		/* next in chain */
} connection;

typedef struct
{ long	       magic;			/* magic code */
  connection  *connection;		/* connection used */
  HENV	       henv;			/* ODBC environment */
  HSTMT	       hstmt;			/* ODBC statement handle */
  RETCODE      rc;			/* status of last operation */
  parameter   *params;			/* Input parameters */
  parameter   *result;			/* Outputs (row descriptions) */
  SQLSMALLINT  NumCols;			/* # columns */
  SQLSMALLINT  NumParams;		/* # parameters */
  functor_t    db_row;			/* Functor for row */
  SQLINTEGER   sqllen;			/* length of statement */
  char        *sqltext;			/* statement text */
  unsigned     flags;			/* general flags */
  nulldef     *null;			/* Prolog null value */
  findall     *findall;			/* compiled code to create result */
  SQLULEN      max_nogetdata;		/* handle as long field if larger */
  struct context *clones;		/* chain of clones */
} context;

static struct
{ long	statements_created;		/* # created statements */
  long  statements_freed;		/* # destroyed statements */
} statistics;


#define CON_MAGIC      0x7c42b620	/* magic code */
#define CTX_MAGIC      0x7c42b621	/* magic code */
#define CTX_FREEMAGIC  0x7c42b622	/* magic code if freed */

#define CTX_PERSISTENT  0x0001		/* persistent statement handle */
#define CTX_BOUND       0x0002		/* result-columns are bound */
#define	CTX_SQLMALLOCED 0x0004		/* sqltext is malloced */
#define CTX_INUSE	0x0008		/* statement is running */
#define CTX_OWNNULL	0x0010		/* null-definition is not shared */
#define CTX_SOURCE	0x0020		/* include source of results */
#define CTX_SILENT	0x0040		/* don't produce messages */
#define CTX_PREFETCHED	0x0080		/* we have a prefetched value */
#define CTX_COLUMNS	0x0100		/* this is an SQLColumns() statement */
#define CTX_TABLES	0x0200		/* this is an SQLTables() statement */
#define CTX_GOT_QLEN	0x0400		/* got SQL_MAX_QUALIFIER_NAME_LEN */
#define CTX_NOAUTO	0x0800		/* fetch by hand */

#define FND_SIZE(n)	((size_t)&((findall*)NULL)->codes[n])

#define true(s, f)	((s)->flags & (f))
#define false(s, f)	!true(s, f)
#define set(s, f)	((s)->flags |= (f))
#define clear(s, f)	((s)->flags &= ~(f))

static  HENV henv;			/* environment handle (ODBC) */


/* Prototypes */
static int pl_put_row(term_t, context *);
static int pl_put_column(context *c, int nth, term_t col);
static SWORD CvtSqlToCType(context *ctxt, SQLSMALLINT, SQLSMALLINT);
static void free_context(context *ctx);
static void close_context(context *ctx);
static foreign_t odbc_set_connection(connection *cn, term_t option);
static int get_pltype(term_t t, SWORD *type);
static SWORD get_sqltype_from_atom(atom_t name, SWORD *type);


		 /*******************************
		 *	      ERRORS		*
		 *******************************/

static int
odbc_report(HENV henv, HDBC hdbc, HSTMT hstmt, RETCODE rc)
{ SQLCHAR state[16];			/* Normally 5-character ID */
  SQLINTEGER native;			/* was DWORD */
  SQLCHAR message[SQL_MAX_MESSAGE_LENGTH+1];
  SWORD   msglen;
  RETCODE rce;
  term_t  msg = PL_new_term_ref();

  switch ( (rce=SQLError(henv, hdbc, hstmt, state, &native, message,
			 sizeof(message), &msglen)) )
  { case SQL_NO_DATA_FOUND:
    case SQL_SUCCESS_WITH_INFO:
      if ( rc != SQL_ERROR )
	return TRUE;
      /*FALLTHROUGH*/
    case SQL_SUCCESS:
      if ( msglen > SQL_MAX_MESSAGE_LENGTH )
	msglen = SQL_MAX_MESSAGE_LENGTH; /* TBD: get the rest? */
      PL_unify_term(msg, PL_FUNCTOR, FUNCTOR_odbc3,
			   PL_CHARS,   state,
		           PL_INTEGER, (long)native,
		           PL_NCHARS,  (size_t)msglen, message);
      break;
    case SQL_INVALID_HANDLE:
      return PL_warning("ODBC INTERNAL ERROR: Invalid handle in error");
    default:
      if ( rc != SQL_ERROR )
	return TRUE;
  }

  switch(rc)
  { case SQL_SUCCESS_WITH_INFO:
    { fid_t fid = PL_open_foreign_frame();
      predicate_t pred = PL_predicate("print_message", 2, "user");
      term_t av = PL_new_term_refs(2);

      PL_put_atom(av+0, ATOM_informational);
      PL_put_term(av+1, msg);

      PL_call_predicate(NULL, PL_Q_NORMAL, pred, av);
      PL_discard_foreign_frame(fid);

      return TRUE;
    }
    case SQL_ERROR:
    { term_t ex = PL_new_term_ref();

      PL_unify_term(ex, PL_FUNCTOR, FUNCTOR_error2,
		    PL_TERM, msg,
		    PL_VARIABLE);

      return PL_raise_exception(ex);
    }
    default:
      return PL_warning("Statement returned %d\n", rc);
  }
}

#define TRY(ctxt, stmt, onfail) \
	{ ctxt->rc = (stmt); \
	  if ( !report_status(ctxt) ) \
	  { onfail; \
	    return FALSE; \
	  } \
	}


static int
report_status(context *ctxt)
{ switch(ctxt->rc)
  { case SQL_SUCCESS:
      return TRUE;
    case SQL_SUCCESS_WITH_INFO:
      if ( true(ctxt, CTX_SILENT) )
	return TRUE;
      break;
    case SQL_NO_DATA_FOUND:
      return FALSE;
    case SQL_INVALID_HANDLE:
      return PL_warning("Invalid handle: %p", ctxt->hstmt);
  }

  return odbc_report(ctxt->henv, ctxt->connection->hdbc,
		     ctxt->hstmt, ctxt->rc);
}


static int
type_error(term_t actual, const char *expected)
{ term_t ex = PL_new_term_ref();

  PL_unify_term(ex, PL_FUNCTOR, FUNCTOR_error2,
		      PL_FUNCTOR, FUNCTOR_type_error2,
		        PL_CHARS, expected,
		        PL_TERM, actual,
		      PL_VARIABLE);

  return PL_raise_exception(ex);
}

static int
domain_error(term_t actual, const char *expected)
{ term_t ex = PL_new_term_ref();

  PL_unify_term(ex, PL_FUNCTOR, FUNCTOR_error2,
		      PL_FUNCTOR, FUNCTOR_domain_error2,
		        PL_CHARS, expected,
		        PL_TERM, actual,
		      PL_VARIABLE);

  return PL_raise_exception(ex);
}

static int
existence_error(term_t actual, const char *expected)
{ term_t ex = PL_new_term_ref();

  PL_unify_term(ex, PL_FUNCTOR, FUNCTOR_error2,
		      PL_FUNCTOR, FUNCTOR_existence_error2,
		        PL_CHARS, expected,
		        PL_TERM, actual,
		      PL_VARIABLE);

  return PL_raise_exception(ex);
}

static int
resource_error(const char *error)
{ term_t ex = PL_new_term_ref();

  PL_unify_term(ex, PL_FUNCTOR, FUNCTOR_error2,
		      PL_FUNCTOR, FUNCTOR_resource_error1,
		        PL_CHARS, error,
		      PL_VARIABLE);

  return PL_raise_exception(ex);
}


static int
representation_error(term_t t, const char *error)
{ term_t ex = PL_new_term_ref();

  PL_unify_term(ex, PL_FUNCTOR, FUNCTOR_error2,
		      PL_FUNCTOR, FUNCTOR_representation_error1,
		        PL_CHARS, error,
		      PL_TERM, t);

  return PL_raise_exception(ex);
}


static int
context_error(term_t term, const char *error, const char *what)
{ term_t ex = PL_new_term_ref();

  PL_unify_term(ex, PL_FUNCTOR, FUNCTOR_error2,
		      PL_FUNCTOR, FUNCTOR_context_error3,
			PL_TERM, term,
		        PL_CHARS, error,
			PL_CHARS, what,
		      PL_VARIABLE);

  return PL_raise_exception(ex);
}


static int
permission_error(const char *op, const char *type, term_t obj)
{ term_t ex = PL_new_term_ref();

  PL_unify_term(ex, PL_FUNCTOR, FUNCTOR_error2,
		      PL_FUNCTOR, FUNCTOR_permission_error3,
		        PL_CHARS, op,
			PL_CHARS, type,
			PL_TERM, obj,
		      PL_VARIABLE);

  return PL_raise_exception(ex);
}


static void *
odbc_malloc(size_t bytes)
{ void *ptr = malloc(bytes);

  if ( !ptr )
    resource_error("memory");

  return ptr;
}



		 /*******************************
		 *	     PRIMITIVES		*
		 *******************************/

#define get_name_arg_ex(i, t, n)  \
	PL_get_typed_arg_ex(i, t, PL_get_atom_chars, "atom", n)
#define get_text_arg_ex(i, t, n)  \
	PL_get_typed_arg_ex(i, t, get_text, "text", n)
#define get_atom_arg_ex(i, t, n)  \
	PL_get_typed_arg_ex(i, t, PL_get_atom, "atom", n)
#define get_int_arg_ex(i, t, n)   \
	PL_get_typed_arg_ex(i, t, PL_get_integer, "integer", n)
#define get_long_arg_ex(i, t, n)   \
	PL_get_typed_arg_ex(i, t, PL_get_long, "integer", n)
#define get_bool_arg_ex(i, t, n)   \
	PL_get_typed_arg_ex(i, t, PL_get_bool, "boolean", n)
#define get_float_arg_ex(i, t, n) \
	PL_get_typed_arg_ex(i, t, PL_get_float, "float", n)

static int
get_text(term_t t, char **s)
{ return PL_get_chars(t, s, CVT_ATOM|CVT_STRING|CVT_LIST|BUF_RING);
}

static int
PL_get_typed_arg_ex(int i, term_t t, int (*func)(), const char *ex, void *ap)
{ term_t a = PL_new_term_ref();

  if ( !PL_get_arg(i, t, a) )
    return type_error(t, "compound");
  if ( !(*func)(a, ap) )
    return type_error(a, ex);

  return TRUE;
}

#define get_int_arg(i, t, n)   \
	PL_get_typed_arg(i, t, PL_get_integer, n)

static int
PL_get_typed_arg(int i, term_t t, int (*func)(), void *ap)
{ term_t a = PL_new_term_ref();

  if ( !PL_get_arg(i, t, a) )
    return FALSE;
  return (*func)(a, ap);
}


static int
list_length(term_t list)
{ term_t tail = PL_copy_term_ref(list);
  term_t head = PL_new_term_ref();
  int n = 0;

  while(PL_get_list(tail, head, tail))
    n++;

  if ( !PL_get_nil(tail) )
  { type_error(tail, "list");
    return -1;
  }

  return n;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
int formatted_string(+Fmt-[Arg...], *len, char **out)
    Much like sformat, but this approach avoids avoids creating
    intermediate Prolog data.  Maybe we should publish pl_format()?
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
formatted_string(term_t in, size_t *len, char **out)
{ term_t av = PL_new_term_refs(3);
  static predicate_t format;
  IOSTREAM *fd = Sopenmem(out, len, "w");

  if ( !format )
    format = PL_predicate("format", 3, "user");
  PL_unify_stream(av+0, fd);
  PL_get_arg(1, in, av+1);
  PL_get_arg(2, in, av+2);
					/* TBD: call natively */
  if ( !PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, format, av) )
  { Sclose(fd);
    if ( *out )
      PL_free(*out);
    return FALSE;
  }

  Sclose(fd);
  return TRUE;
}


		 /*******************************
		 *	    NULL VALUES		*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
There are many ways one  may  wish   to  handle  SQL  null-values. These
functions deal with the three common ways   specially  and can deal with
arbitrary representations.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static nulldef *
nulldef_spec(term_t t)
{ atom_t a;
  functor_t f;
  nulldef *nd;

  if ( !(nd=odbc_malloc(sizeof(*nd))) )
    return NULL;

  memset(nd, 0, sizeof(*nd));

  if ( PL_get_atom(t, &a) )
  { if ( a == ATOM_null )
    { free(nd);				/* TBD: not very elegant */
      return NULL;			/* default specifier */
    }
    nd->nulltype  = NULL_ATOM;
    nd->nullvalue.atom = a;
    PL_register_atom(a);		/* avoid atom-gc */
  } else if ( PL_is_variable(t) )
  { nd->nulltype = NULL_VAR;
  } else if ( PL_get_functor(t, &f) &&
	      PL_functor_arity(f) == 1 )
  { term_t a1 = PL_new_term_ref();

    PL_get_arg(1, t, a1);
    if ( PL_is_variable(a1) )
    { nd->nulltype = NULL_FUNCTOR;
      nd->nullvalue.functor = f;
    } else
      goto term;
  } else
  { term:
    nd->nulltype = NULL_RECORD;
    nd->nullvalue.record = PL_record(t);
  }

  nd->references = 1;

  return nd;
}


static nulldef *
clone_nulldef(nulldef *nd)
{ if ( nd )
    nd->references++;

  return nd;
}


static void
free_nulldef(nulldef *nd)
{ if ( nd && --nd->references == 0 )
  { switch(nd->nulltype)
    { case NULL_ATOM:
	PL_unregister_atom(nd->nullvalue.atom);
        break;
      case NULL_RECORD:
	PL_erase(nd->nullvalue.record);
        break;
      default:
	break;
    }

    free(nd);
  }
}


static void
put_sql_null(term_t t, nulldef *nd)
{ if ( nd )
  { switch(nd->nulltype)
    { case NULL_VAR:
	break;
      case NULL_ATOM:
	PL_put_atom(t, nd->nullvalue.atom);
        break;
      case NULL_FUNCTOR:
	PL_put_functor(t, nd->nullvalue.functor);
        break;
      case NULL_RECORD:
	PL_recorded(nd->nullvalue.record, t);
    }
  } else
    PL_put_atom(t, ATOM_null);
}


static int
is_sql_null(term_t t, nulldef *nd)
{ if ( nd )
  { switch(nd->nulltype)
    { case NULL_VAR:
	return PL_is_variable(t);
      case NULL_ATOM:
      { atom_t a;

	return PL_get_atom(t, &a) && a == nd->nullvalue.atom;
      }
      case NULL_FUNCTOR:
	return PL_is_functor(t, nd->nullvalue.functor);
      case NULL_RECORD:			/* TBD: Provide PL_unify_record */
      { term_t rec = PL_new_term_ref();
	PL_recorded(nd->nullvalue.record, rec);
	return PL_unify(t, rec);
      }
      default:				/* should not happen */
	assert(0);
        return FALSE;
    }
  } else
  { atom_t a;

    return PL_get_atom(t, &a) && a == ATOM_null;
  }
}

		 /*******************************
		 *   FINDALL(Term, row(X,...))	*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
This section deals with  the  implementation   of  the  statement option
findall(Template, row(Column,...)), returning a  list   of  instances of
Template for each row.

Ideally, we should unify the row with   the  second argument and add the
first to the list. Unfortunately, we have   to  make fresh copies of the
findall/2 term for this to work, or we must protect the Template using a
record. Both approaches are slow and largely  discard the purpose of the
option, which is to avoid findall/3 and its associated costs in terms of
copying and memory fragmentation.

The current implementation is incomplete. It does not allow arguments of
row(...) to be instantiated. Plain instantiation   can always be avoided
using a proper SELECT statement. Potentionally   useful however would be
the  translation  of   compound   terms,    especially   to   translates
date/time/timestamp structures to a format for use by the application.

The  statement  is  compiled  into  a    findall  statement,  a  set  of
instructions that builds the target structure   from the row returned by
the current statement.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#define MAXCODES 256
#define ROW_ARG  1024			/* way above Prolog types */

typedef struct
{ term_t row;				/* the row */
  term_t tmp;				/* scratch term */
  int columns;				/* arity of row-term */
  unsigned flags;			/* CTX_PERSISTENT */
  int  size;				/* # codes */
  code buf[MAXCODES];
} compile_info;

#define ADDCODE(info, val) (info->buf[info->size++] = (code)(val))
#define ADDCODE_1(info, v1, v2) ADDCODE(info, v1), ADDCODE(info, v2)

static int
nth_row_arg(compile_info *info, term_t var)
{ int i;

  for(i=1; i<=info->columns; i++)
  { PL_get_arg(i, info->row, info->tmp);
    if ( PL_compare(info->tmp, var) == 0 )
      return i;
  }

  return 0;
}


typedef union
{ code   ascode[sizeof(double)/sizeof(code)];
  double asdouble;
} u_double;


static int
compile_arg(compile_info *info, term_t t)
{ int tt;

  switch((tt=PL_term_type(t)))
  { case PL_VARIABLE:
    { int nth;

      if ( (nth=nth_row_arg(info, t)) )
      { ADDCODE_1(info, ROW_ARG, nth);
      } else
	ADDCODE(info, PL_VARIABLE);
      break;
    }
    case PL_ATOM:
    { atom_t val;
      PL_get_atom(t, &val);
      ADDCODE_1(info, PL_ATOM, val);
      if ( true(info, CTX_PERSISTENT) )
	PL_register_atom(val);
      break;
    }
    case PL_STRING:
    case PL_FLOAT:
      if ( true(info, CTX_PERSISTENT) )
      { if ( tt == PL_FLOAT )
	{ u_double v;
	  unsigned int i;

	  PL_get_float(t, &v.asdouble);
	  ADDCODE(info, PL_FLOAT);
	  for(i=0; i<sizeof(double)/sizeof(code); i++)
	    ADDCODE(info, v.ascode[i]);
	} else				/* string */
	{ size_t len;
	  char *s, *cp;

	  PL_get_string_chars(t, &s, &len);
	  if ( !(cp = odbc_malloc(len+1)) )
	    return FALSE;
	  memcpy(cp, s, len+1);
	  ADDCODE(info, PL_STRING);
	  ADDCODE(info, len);
	  ADDCODE(info, cp);
	}
      } else
      { term_t cp = PL_copy_term_ref(t);
	ADDCODE_1(info, PL_TERM, cp);
      }
      break;
    case PL_INTEGER:
    { long v;
      PL_get_long(t, &v);
      ADDCODE_1(info, PL_INTEGER, v);
      break;
    }
    case PL_TERM:
    { functor_t f;
      int i, arity;
      term_t a = PL_new_term_ref();

      PL_get_functor(t, &f);
      arity = PL_functor_arity(f);
      ADDCODE_1(info, PL_FUNCTOR, f);
      for(i=1; i<=arity; i++)
      { PL_get_arg(i, t, a);
	compile_arg(info, a);
      }
      break;
    }
    default:
      assert(0);
  }

  return TRUE;
}


static findall *
compile_findall(term_t all, unsigned flags)
{ compile_info info;
  term_t t = PL_new_term_ref();
  atom_t a;
  findall *f;
  int i;

  info.tmp   = PL_new_term_ref();
  info.row   = PL_new_term_ref();
  info.size  = 0;
  info.flags = flags;

  PL_get_arg(2, all, info.row);
  PL_get_name_arity(info.row, &a, &info.columns);

  for(i=1; i<=info.columns; i++)
  { PL_get_arg(i, info.row, t);
    if ( !PL_is_variable(t) )
    { type_error(t, "unbound");
      return NULL;
    }
  }

  PL_get_arg(1, all, t);
  compile_arg(&info, t);

  if ( !(f = odbc_malloc(FND_SIZE(info.size))) )
    return NULL;
  f->references = 1;
  f->flags = flags;
  memcpy(f->codes, info.buf, sizeof(code)*info.size);

  return f;
}


static findall *
clone_findall(findall *in)
{ if ( in )
    in->references++;
  return in;
}


static code *
unregister_code(code *PC)
{ switch((int)*PC++)
  { case PL_VARIABLE:
      return PC;
    case ROW_ARG:			/* 1-based column */
    case PL_INTEGER:
    case PL_TERM:
      return PC+1;
    case PL_ATOM:
      PL_unregister_atom((atom_t)*PC++);
      return PC;
    case PL_FLOAT:
      return PC+sizeof(double)/sizeof(code);
    case PL_STRING:
    { char *s = (char*)PC[2];
      free(s);
      return PC+2;
    }
    case PL_FUNCTOR:
    { functor_t f = (functor_t)*PC++;
      int i, arity = PL_functor_arity(f);

      for(i=0;i<arity;i++)
      { if ( !(PC=unregister_code(PC)) )
	  return NULL;
      }

      return PC;
    }
    default:
      assert(0);
      return NULL;
  }
}


static void
free_findall(findall *in)
{ if ( in && --in->references == 0 )
  { if ( true(in, CTX_PERSISTENT) )
      unregister_code(in->codes);

    free(in);
  }
}


static code *
build_term(context *ctxt, code *PC, term_t result)
{ switch((int)*PC++)
  { case PL_VARIABLE:
      return PC;
    case ROW_ARG:			/* 1-based column */
    { int column = (int)*PC++;
      if ( pl_put_column(ctxt, column-1, result) )
	return PC;
      return NULL;
    }
    case PL_ATOM:
    { PL_put_atom(result, (atom_t)*PC++);
      return PC;
    }
    case PL_FLOAT:
    { u_double v;
      unsigned int i;

      for(i=0; i<sizeof(double)/sizeof(code); i++)
	v.ascode[i] = *PC++;
      PL_put_float(result, v.asdouble);
      return PC;
    }
    case PL_STRING:
    { int len = (int)*PC++;
      char *s = (char*)*PC++;
      PL_put_string_nchars(result, len, s);
      return PC;
    }
    case PL_INTEGER:
    { PL_put_integer(result, (long)*PC++);
      return PC;
    }
    case PL_TERM:
    { PL_put_term(result, (term_t)*PC++);
      return PC;
    }
    case PL_FUNCTOR:
    { functor_t f = (functor_t)*PC++;
      int i, arity = PL_functor_arity(f);
      term_t av = PL_new_term_refs(arity);

      for(i=0;i<arity;i++)
      { if ( !(PC=build_term(ctxt, PC, av+i)) )
	  return NULL;
      }

      PL_cons_functor_v(result, f, av);
      PL_reset_term_refs(av);
      return PC;
    }
    default:
      assert(0);
      return NULL;
  }
}


static int
put_findall(context *ctxt, term_t result)
{ if ( build_term(ctxt, ctxt->findall->codes, result) )
    return TRUE;

  return FALSE;
}



		 /*******************************
		 *	    CONNECTION		*
		 *******************************/

static connection *connections;

static connection *
find_connection(atom_t alias)
{ connection *c;

  for(c=connections; c; c=c->next)
  { if ( c->alias == alias )
      return c;
  }


  return NULL;
}


static connection *
find_connection_from_dsn(atom_t dsn)
{ connection *c;

  for(c=connections; c; c=c->next)
  { if ( c->dsn == dsn )
      return c;
  }

  return NULL;
}


static connection *
alloc_connection(atom_t alias, atom_t dsn)
{ connection *c;

  if ( alias && find_connection(alias) )
    return NULL;			/* already existenting */

  if ( !(c = odbc_malloc(sizeof(*c))) )
    return NULL;
  memset(c, 0, sizeof(*c));
  c->alias = alias;
  if ( alias )
    PL_register_atom(alias);
  c->dsn = dsn;
  PL_register_atom(dsn);
  c->max_nogetdata = MAX_NOGETDATA;

  c->next = connections;
  connections = c;

  return c;
}


static void
free_connection(connection *c)
{ if ( c == connections )
    connections = c->next;
  else
  { connection *c2;

    for(c2 = connections; c2; c2 = c2->next)
    { if ( c2->next == c )
      { c2->next = c->next;
	break;
      }
    }
  }

  if ( c->alias )
    PL_unregister_atom(c->alias);
  if ( c->dsn )
    PL_unregister_atom(c->dsn);
  free_nulldef(c->null);

  free(c);
}


static int
get_connection(term_t tdsn, connection **cn)
{ atom_t dsn;
  connection *c;

  if ( PL_is_functor(tdsn, FUNCTOR_odbc_connection1) )
  { term_t a = PL_new_term_ref();
    void *ptr;

    PL_get_arg(1, tdsn, a);
    if ( !PL_get_pointer(a, &ptr) )
      return type_error(tdsn, "odbc_connection");
    c = ptr;

    if ( !c->magic == CON_MAGIC )
      return existence_error(tdsn, "odbc_connection");
  } else
  { if ( !PL_get_atom(tdsn, &dsn) )
      return type_error(tdsn, "odbc_connection");
    if ( !(c=find_connection(dsn)) )
      return existence_error(tdsn, "odbc_connection");
  }

  *cn = c;

  return TRUE;
}


static int
unify_connection(term_t t, connection *cn)
{ if ( cn->alias )
    return PL_unify_atom(t, cn->alias);

  return PL_unify_term(t, PL_FUNCTOR, FUNCTOR_odbc_connection1,
		            PL_POINTER, cn);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
odbc_connect(+DSN, -Connection, +Options)
    Create a new connection. Option is a list of options with the
    following standards:

	user(User)

	password(Password)

	alias(Name)
	    Alias-name for the connection.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#define MAX_AFTER_OPTIONS 10

static foreign_t
pl_odbc_connect(term_t tdsource, term_t cid, term_t options)
{  atom_t dsn;
   const char *dsource;			/* odbc data source */
   char *uid = NULL;			/* user id */
   char *pwd = NULL;			/* password */
   char *driver_string = NULL;			/* driver_string */
   atom_t alias = 0;			/* alias-name */
   int mars = 0;			/* mars-value */
   atom_t open = 0;			/* open next connection */
   RETCODE rc;				/* result code for ODBC functions */
   HDBC hdbc;
   connection *cn;
   term_t tail = PL_copy_term_ref(options);
   term_t head = PL_new_term_ref();
   term_t after_open = PL_new_term_refs(MAX_AFTER_OPTIONS);
   int i, nafter = 0;
   int silent = FALSE;

   /* Read parameters from terms. */
   if ( !PL_get_atom(tdsource, &dsn) )
     return type_error(tdsource, "atom");

   while(PL_get_list(tail, head, tail))
   { if ( PL_is_functor(head, FUNCTOR_user1) )
     { if ( !get_name_arg_ex(1, head, &uid) )
	 return FALSE;
     } else if ( PL_is_functor(head, FUNCTOR_password1) )
     { if ( !get_text_arg_ex(1, head, &pwd) )
	 return FALSE;
     } else if ( PL_is_functor(head, FUNCTOR_alias1) )
     { if ( !get_atom_arg_ex(1, head, &alias) )
	 return FALSE;
     } else if ( PL_is_functor(head, FUNCTOR_driver_string1) )
     { if ( !get_text_arg_ex(1, head, &driver_string) )
	 return FALSE;
     } else if ( PL_is_functor(head, FUNCTOR_mars1) )
     { if ( !get_bool_arg_ex(1, head, &mars) )
	 return FALSE;
     } else if ( PL_is_functor(head, FUNCTOR_open1) )
     { if ( !get_atom_arg_ex(1, head, &open) )
	 return FALSE;
       if ( !(open == ATOM_once ||
	      open == ATOM_multiple) )
	 return domain_error(head, "open_mode");
     } else if ( PL_is_functor(head, FUNCTOR_silent1) )
     { if ( !get_bool_arg_ex(1, head, &silent) )
	 return FALSE;
     } else if ( PL_is_functor(head, FUNCTOR_auto_commit1) ||
		 PL_is_functor(head, FUNCTOR_null1) ||
		 PL_is_functor(head, FUNCTOR_access_mode1) ||
		 PL_is_functor(head, FUNCTOR_cursor_type1) ||
		 PL_is_functor(head, FUNCTOR_wide_column_threshold1) )
     { if ( nafter < MAX_AFTER_OPTIONS )
	 PL_put_term(after_open+nafter++, head);
       else
	 return PL_warning("Too many options"); /* shouldn't happen */
     } else
       return domain_error(head, "odbc_option");
   }
   if ( !PL_get_nil(tail) )
     return type_error(tail, "list");

   if ( !open )
     open = alias ? ATOM_once : ATOM_multiple;
   if ( open == ATOM_once && (cn = find_connection_from_dsn(dsn)) )
   { if ( alias && cn->alias != alias )
     { if ( !cn->alias )
       { if ( !find_connection(alias) )
	 { cn->alias = alias;
	   PL_register_atom(alias);
	 } else
	   return PL_warning("Alias already in use");
       } else
	 return PL_warning("Cannot redefined connection alias");
     }
     return unify_connection(cid, cn);
   }

   dsource = PL_atom_chars(dsn);

   if ( !henv )
   { if ( (rc=SQLAllocEnv(&henv)) != SQL_SUCCESS )
       return PL_warning("Could not initialise SQL environment");
   }

   if ( (rc=SQLAllocConnect(henv, &hdbc)) != SQL_SUCCESS )
     return odbc_report(henv, NULL, NULL, rc);

   if ( mars )
   { if ( (rc=SQLSetConnectAttr(hdbc,
				SQL_COPT_SS_MARS_ENABLED,
				SQL_MARS_ENABLED_YES,
				SQL_IS_UINTEGER)) != SQL_SUCCESS )
     return odbc_report(henv, NULL, NULL, rc);
   }

   /* Connect to a data source. */
   if ( driver_string != NULL )
   { if ( uid != NULL )
     { return context_error(options, "Option incompatible with driver_string",
			    "user");
     } else if ( pwd != NULL )
     { return context_error(options, "Option incompatible with driver_string",
			    "password");
     } else
     { SQLCHAR connection_out[1025];	/* completed driver string */
       SQLSMALLINT connection_out_len;

       rc = SQLDriverConnect(hdbc,
                             NULL, /* window handle */
                             (SQLCHAR *)driver_string, SQL_NTS,
                             connection_out, 1024,
                             &connection_out_len,
                             SQL_DRIVER_NOPROMPT);
     }
   } else
   { rc = SQLConnect(hdbc, (SQLCHAR *)dsource, SQL_NTS,
                           (SQLCHAR *)uid,     SQL_NTS,
                           (SQLCHAR *)pwd,     SQL_NTS);
   }
   if ( rc == SQL_ERROR )
     return odbc_report(henv, hdbc, NULL, rc);
   if ( rc != SQL_SUCCESS && !silent && !odbc_report(henv, hdbc, NULL, rc) )
     return FALSE;

   if ( !(cn=alloc_connection(alias, dsn)) )
     return FALSE;
   if ( silent )
     set(cn, CTX_SILENT);

   cn->hdbc = hdbc;

   if ( !unify_connection(cid, cn) )
   { free_connection(cn);
     return FALSE;
   }

   DEBUG(3, Sdprintf("Processing %d `after' options\n", nafter));
   for(i=0; i<nafter; i++)
   { if ( !odbc_set_connection(cn, after_open+i) )
     { free_connection(cn);
       return FALSE;
     }
   }

   return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
odbc_disconnect(+Connection)
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#define TRY_CN(cn, action) \
	{ RETCODE rc = action; \
	  if ( rc != SQL_SUCCESS ) \
	    return odbc_report(henv, cn->hdbc, NULL, rc); \
	}


static foreign_t
pl_odbc_disconnect(term_t dsn)
{ connection *cn;

  if ( !get_connection(dsn, &cn) )
    return FALSE;

  TRY_CN(cn, SQLDisconnect(cn->hdbc));  /* Disconnect from the data source */
  TRY_CN(cn, SQLFreeConnect(cn->hdbc)); /* Free the connection handle */
  free_connection(cn);

  return TRUE;
}


static foreign_t
odbc_current_connection(term_t cid, term_t dsn, control_t h)
{ connection *cn;

  switch(PL_foreign_control(h))
  { case PL_FIRST_CALL:
      if ( !PL_is_variable(cid) )
      { if ( get_connection(cid, &cn) &&
	     PL_unify_atom(dsn, cn->dsn) )
	  return TRUE;

	return FALSE;
      }

      cn = connections;
      goto next;

    case PL_REDO:
      cn = PL_foreign_context_address(h);
    next:
    { fid_t fid = PL_open_foreign_frame();

      for(; cn; cn = cn->next, PL_rewind_foreign_frame(fid))
      { if ( unify_connection(cid, cn) &&
	     PL_unify_atom(dsn, cn->dsn) )
	{ PL_close_foreign_frame(fid);
	  if ( cn->next )
	    PL_retry_address(cn->next);
	  return TRUE;
	}
      }

      PL_close_foreign_frame(fid);
      return FALSE;
    }

    default:
      return FALSE;
  }
}


static foreign_t
odbc_set_connection(connection *cn, term_t option)
{ RETCODE rc;
  UWORD opt;
  UDWORD optval;

  if ( PL_is_functor(option, FUNCTOR_auto_commit1) )
  { int val;

    if ( !get_bool_arg_ex(1, option, &val) )
      return FALSE;
    opt = SQL_AUTOCOMMIT;
    optval = (val ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF);
  } else if ( PL_is_functor(option, FUNCTOR_access_mode1) )
  { atom_t val;

    if ( !get_atom_arg_ex(1, option, &val) )
      return FALSE;
    opt = SQL_ACCESS_MODE;

    if ( val == ATOM_read )
      optval = SQL_MODE_READ_ONLY;
    else if ( val == ATOM_update )
      optval = SQL_MODE_READ_WRITE;
    else
      return domain_error(val, "access_mode");
  } else if ( PL_is_functor(option, FUNCTOR_cursor_type1) )
  { atom_t val;

    if ( !get_atom_arg_ex(1, option, &val) )
      return FALSE;
    opt = SQL_CURSOR_TYPE;

    if ( val == ATOM_dynamic )
      optval = SQL_CURSOR_DYNAMIC;
    else if ( val == ATOM_forwards_only )
      optval = SQL_CURSOR_FORWARD_ONLY;
    else if ( val == ATOM_keyset_driven )
      optval = SQL_CURSOR_KEYSET_DRIVEN;
    else if ( val == ATOM_static )
      optval = SQL_CURSOR_STATIC;
    else
      return domain_error(val, "cursor_type");
  } else if ( PL_is_functor(option, FUNCTOR_silent1) )
  { int val;

    if ( !get_bool_arg_ex(1, option, &val) )
      return FALSE;

    set(cn, CTX_SILENT);

    return TRUE;
  } else if ( PL_is_functor(option, FUNCTOR_null1) )
  { term_t a = PL_new_term_ref();

    PL_get_arg(1, option, a);
    cn->null = nulldef_spec(a);

    return TRUE;
  } else if ( PL_is_functor(option, FUNCTOR_wide_column_threshold1) )
  { int val;

    if ( !get_int_arg_ex(1, option, &val) )
      return FALSE;
    DEBUG(2, Sdprintf("Using wide_column_threshold = %d\n", val));
    cn->max_nogetdata = val;

    return TRUE;
  } else
    return domain_error(option, "odbc_option");

  if ( (rc=SQLSetConnectOption(cn->hdbc, opt, optval)) != SQL_SUCCESS )
    return odbc_report(henv, cn->hdbc, NULL, rc);

  return TRUE;
}


static foreign_t
pl_odbc_set_connection(term_t con, term_t option)
{ connection *cn;

  if ( !get_connection(con, &cn) )
    return FALSE;

  return odbc_set_connection(cn, option);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Options for SQLGetInfo() from http://msdn.microsoft.com/library/default.asp?url=/library/en-us/odbcsql/od_odbc_c_9qp1.asp
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

typedef struct
{ const char *name;
  UWORD id;
  enum { text, sword } type;
  functor_t functor;
} conn_option;

static conn_option conn_option_list[] =
{ { "database_name",	   SQL_DATABASE_NAME, text },
  { "dbms_name",           SQL_DBMS_NAME, text },
  { "dbms_version",        SQL_DBMS_VER, text },
  { "driver_name",         SQL_DRIVER_NAME, text },
  { "driver_odbc_version", SQL_DRIVER_ODBC_VER, text },
  { "driver_version",      SQL_DRIVER_VER, text },
  { "active_statements",   SQL_ACTIVE_STATEMENTS, sword },
  { NULL, 0 }
};

static foreign_t
odbc_get_connection(term_t conn, term_t option, control_t h)
{ connection *cn;
  conn_option *opt;
  functor_t f;
  term_t a, val;

  switch(PL_foreign_control(h))
  { case PL_FIRST_CALL:
      if ( !get_connection(conn, &cn) )
	return FALSE;

      opt = conn_option_list;

      if ( PL_get_functor(option, &f) )
      {	goto find;
      }	else if ( PL_is_variable(option) )
      { f = 0;
	goto find;
      } else
	return type_error(option, "odbc_option");
    case PL_REDO:
      if ( !get_connection(conn, &cn) )
	return FALSE;

      f = 0;
      opt = PL_foreign_context_address(h);

      goto find;
    case PL_CUTTED:
    default:
      return TRUE;
  }

find:
  val = PL_new_term_ref();
  a = PL_new_term_ref();
  PL_get_arg(1, option, a);

  for(; opt->name; opt++)
  { if ( !opt->functor )
      opt->functor = PL_new_functor(PL_new_atom(opt->name), 1);

    if ( !f || opt->functor == f )
    { char buf[256];
      SWORD len;
      RETCODE rc;

      if ( (rc=SQLGetInfo(cn->hdbc, opt->id,
			  buf, sizeof(buf), &len)) != SQL_SUCCESS )
      { if ( f )
	  return odbc_report(henv, cn->hdbc, NULL, rc);
	else
	  continue;
      }

      switch( opt->type )
      { case text:
	  PL_put_atom_nchars(val, len, buf);
	  break;
	case sword:
	{ SQLSMALLINT v = *((SQLSMALLINT*)buf);
	  PL_put_integer(val, v);
	  break;
	}
	default:
	  assert(0);
	return FALSE;
      }


      if ( f )
	return PL_unify(a, val);

      PL_unify_term(option,
		    PL_FUNCTOR, opt->functor,
		    PL_TERM, val);

      if ( opt[1].name )
	PL_retry_address(opt+1);
      else
	return TRUE;
    }
  }

  if ( f )
    return domain_error(option, "odbc_option");

  return FALSE;
}


static foreign_t
odbc_end_transaction(term_t conn, term_t action)
{ connection *cn;
  RETCODE rc;
  UWORD opt;
  atom_t a;


  if ( !get_connection(conn, &cn) )
    return FALSE;

  if ( !PL_get_atom(action, &a) )
    return type_error(action, "atom");
  if ( a == ATOM_commit )
  { opt = SQL_COMMIT;
  } else if ( a == ATOM_rollback )
  { opt = SQL_ROLLBACK;
  } else
    return domain_error(action, "transaction");

  if ( (rc=SQLTransact(henv, cn->hdbc, opt)) != SQL_SUCCESS )
    return odbc_report(henv, cn->hdbc, NULL, rc);

  return TRUE;
}


		 /*******************************
		 *	CONTEXT (STATEMENTS)	*
		 *******************************/

static context *
new_context(connection *cn)
{ context *ctxt = odbc_malloc(sizeof(context));
  RETCODE rc;

  if ( !ctxt )
    return NULL;
  memset(ctxt, 0, sizeof(*ctxt));
  ctxt->magic = CTX_MAGIC;
  ctxt->henv  = henv;
  ctxt->connection = cn;
  ctxt->null = cn->null;
  ctxt->flags = cn->flags;
  ctxt->max_nogetdata = cn->max_nogetdata;
  if ( (rc=SQLAllocStmt(cn->hdbc, &ctxt->hstmt)) != SQL_SUCCESS )
  { odbc_report(henv, cn->hdbc, NULL, rc);
    return NULL;
  }
  statistics.statements_created++;

  return ctxt;
}


static void
close_context(context *ctxt)
{ clear(ctxt, CTX_INUSE);

  if ( ctxt->flags & CTX_PERSISTENT )
  { if ( ctxt->hstmt )
    { ctxt->rc = SQLFreeStmt(ctxt->hstmt, SQL_CLOSE);
      if ( ctxt->rc == SQL_ERROR )
	report_status(ctxt);
    }
  } else
    free_context(ctxt);
}


static void
free_parameters(int n, parameter *params)
{ if ( n && params )
  { parameter *p = params;
    int i;

    for (i=0; i<n; i++, p++)
    { if ( p->ptr_value &&
	   p->ptr_value != (void *)p->buf &&
	   p->len_value != SQL_LEN_DATA_AT_EXEC(0) ) /* Using SQLPutData() */
	free(p->ptr_value);
      if ( p->source.table )
	PL_unregister_atom(p->source.table);
      if ( p->source.column )
	PL_unregister_atom(p->source.column);
    }

    free(params);
  }
}


static void
free_context(context *ctx)
{ if ( ctx->magic != CTX_MAGIC )
  { if ( ctx->magic == CTX_FREEMAGIC )
      Sdprintf("ODBC: Trying to free context twice: %p\n", ctx);
    else
      Sdprintf("ODBC: Trying to free non-context: %p\n", ctx);

    return;
  }

  ctx->magic = CTX_FREEMAGIC;

  if ( ctx->hstmt )
  { ctx->rc = SQLFreeStmt(ctx->hstmt, SQL_DROP);
    if ( ctx->rc == SQL_ERROR )
      report_status(ctx);
  }

  free_parameters(ctx->NumCols,   ctx->result);
  free_parameters(ctx->NumParams, ctx->params);
  if ( true(ctx, CTX_SQLMALLOCED) )
    PL_free(ctx->sqltext);
  if ( true(ctx, CTX_OWNNULL) )
    free_nulldef(ctx->null);
  if ( ctx->findall )
    free_findall(ctx->findall);
  free(ctx);

  statistics.statements_freed++;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
clone_context()

Create a clone of a context, so we   can have the same statement running
multiple times. Is there really no better   way  to handle this? Can't I
have multiple cursors on one statement?
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static context *
clone_context(context *in)
{ context *new;

  if ( !(new = new_context(in->connection)) )
    return NULL;
					/* Copy SQL statement */
  if ( !(new->sqltext = PL_malloc(in->sqllen+1)) )
    return NULL;
  new->sqllen = in->sqllen;
  memcpy(new->sqltext, in->sqltext, in->sqllen+1);
  set(new, CTX_SQLMALLOCED);

					/* Prepare the statement */
  TRY(new,
      SQLPrepare(new->hstmt, (SQLCHAR*)new->sqltext, new->sqllen),
      close_context(new));

					/* Copy parameter declarations */
  if ( (new->NumParams = in->NumParams) > 0 )
  { int pn;
    parameter *p;

    if ( !(new->params = odbc_malloc(sizeof(parameter)*new->NumParams)) )
      return NULL;
    memcpy(new->params, in->params, sizeof(parameter)*new->NumParams);

    for(p=new->params, pn=1; pn<=new->NumParams; pn++, p++)
    { SQLLEN *vlenptr = NULL;

      switch(p->cTypeID)
      { case SQL_C_CHAR:
	case SQL_C_BINARY:
	  if ( !(p->ptr_value = odbc_malloc(p->length_ind)) )
	    return NULL;
	  vlenptr = &p->len_value;
	  break;
      }

      TRY(new, SQLBindParameter(new->hstmt,		/* hstmt */
				(SWORD)pn,			/* ipar */
				SQL_PARAM_INPUT, 	/* fParamType */
				p->cTypeID, 		/* fCType */
				p->sqlTypeID,		/* fSqlType */
				p->length_ind,		/* cbColDef */
				p->scale,		/* ibScale */
				p->ptr_value,		/* rgbValue */
				0,			/* cbValueMax */
				vlenptr),		/* pcbValue */
	  close_context(new));
    }
  }

					/* Copy result columns */
  new->db_row = in->db_row;		/* the row/N functor */

  if ( in->result )
  { new->NumCols = in->NumCols;
    if ( !(new->result  = odbc_malloc(in->NumCols*sizeof(parameter))) )
      return NULL;
    memcpy(new->result, in->result, in->NumCols*sizeof(parameter));

    if ( true(in, CTX_BOUND) )
    { parameter *p = new->result;
      int i;

      for(i = 1; i <= new->NumCols; i++, p++)
      { if ( p->len_value > PARAM_BUFSIZE )
	{ if ( !(p->ptr_value = odbc_malloc(p->len_value)) )
	    return NULL;
	} else
	  p->ptr_value = (SQLPOINTER)p->buf;

	TRY(new, SQLBindCol(new->hstmt, (SWORD)i,
			    p->cTypeID,
			    p->ptr_value,
			    p->len_value,
			    &p->length_ind),
	    close_context(new));
      }

      set(new, CTX_BOUND);
    }
  }

  new->null    = clone_nulldef(in->null);
  new->findall = clone_findall(in->findall);

  return new;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
The string is malloced by Prolog and   this probably poses problems when
using on Windows, where each DLL  has   its  own memory pool. SWI-Prolog
5.0.9 introduces PL_malloc(), PL_realloc()  and   PL_free()  for foreign
code to synchronise this problem.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
get_sql_text(context *ctxt, term_t tquery)
{ size_t qlen;
  char *q;

  if ( PL_is_functor(tquery, FUNCTOR_minus2) )
  { qlen = 0;
    q = NULL;

    if ( !formatted_string(tquery, &qlen, &q) )
      return FALSE;
    ctxt->sqltext = q;
    ctxt->sqllen = (SQLINTEGER)qlen;
    set(ctxt, CTX_SQLMALLOCED);
  } else if ( PL_get_nchars(tquery, &qlen, &q, CVT_ATOM|CVT_STRING|BUF_MALLOC))
  { ctxt->sqltext = q;
    ctxt->sqllen = (SQLINTEGER)qlen;
    set(ctxt, CTX_SQLMALLOCED);
  } else
    return type_error(tquery, "atom_or_format");

  return TRUE;
}


static int
max_qualifier_lenght(connection *cn)
{ if ( false(cn, CTX_GOT_QLEN) )
  { SQLUSMALLINT len;
    SWORD plen;
    RETCODE rc;

    if ( (rc=SQLGetInfo(cn->hdbc, SQL_MAX_QUALIFIER_NAME_LEN,
			&len, sizeof(len), &plen)) == SQL_SUCCESS )
    { /*Sdprintf("SQL_MAX_QUALIFIER_NAME_LEN = %d\n", (int)len);*/
      cn->max_qualifier_lenght = (int)len; /* 0: unknown */
    } else
    { odbc_report(henv, cn->hdbc, NULL, rc);
      cn->max_qualifier_lenght = -1;
    }

    set(cn, CTX_GOT_QLEN);
  }

  return cn->max_qualifier_lenght;
}


static int
prepare_result(context *ctxt)
{ SQLSMALLINT i;
  SQLCHAR nameBuffer[NameBufferLength];
  SQLSMALLINT nameLength, dataType, decimalDigits, nullable;
  SQLULEN columnSize;			/* was SQLUINTEGER */
  parameter *ptr_result;
  SQLSMALLINT ncol;

#ifdef SQL_SERVER_BUG
  if ( true(ctxt, CTX_COLUMNS) )
    ncol = 12;
  else
#endif
  SQLNumResultCols(ctxt->hstmt, &ncol);
  if ( ncol == 0 )
    return TRUE;			/* no results */

  if ( ctxt->result )			/* specified types */
  { if ( ncol != ctxt->NumCols )
      return PL_warning("# columns mismatch"); /* TBD: exception */
  } else
  { ctxt->NumCols = ncol;
    ctxt->db_row = PL_new_functor(ATOM_row, ctxt->NumCols);
    if ( !(ctxt->result = odbc_malloc(sizeof(parameter)*ctxt->NumCols)) )
      return FALSE;
    memset(ctxt->result, 0, sizeof(parameter)*ctxt->NumCols);
  }

  ptr_result = ctxt->result;
  for(i = 1; i <= ctxt->NumCols; i++, ptr_result++)
  { SQLDescribeCol(ctxt->hstmt, i,
		   nameBuffer, NameBufferLength, &nameLength,
		   &dataType, &columnSize, &decimalDigits,
		   &nullable);

    if ( true(ctxt, CTX_SOURCE) )
    { SQLLEN ival;			/* was DWORD */

      ptr_result->source.column = PL_new_atom_nchars(nameLength,
						     (char*)nameBuffer);
      if ( (ctxt->rc=SQLColAttributes(ctxt->hstmt, i,
				      SQL_COLUMN_TABLE_NAME,
				      nameBuffer,
				      NameBufferLength, &nameLength,
				      &ival)) == SQL_SUCCESS )
      { ptr_result->source.table = PL_new_atom_nchars(nameLength,
						      (char*)nameBuffer);
      } else
      { if ( !report_status(ctxt) )		/* TBD: May close ctxt */
	  return FALSE;
	ptr_result->source.table = ATOM_;
	PL_register_atom(ATOM_);
      }
    }

    ptr_result->sqlTypeID = dataType;
    ptr_result->cTypeID = CvtSqlToCType(ctxt, dataType, ptr_result->plTypeID);
    if (ptr_result->cTypeID == CVNERR)
    { free_context(ctxt);
      return PL_warning("odbc_query/2: column type not managed");
    }

    DEBUG(1, Sdprintf("prepare_result(): column %d, "
		      "sqlTypeID = %d, cTypeID = %d, "
		      "columnSize = %u\n",
		      i, ptr_result->sqlTypeID, ptr_result->cTypeID,
		      columnSize));

    if ( true(ctxt, CTX_TABLES) )
    { switch (ptr_result->sqlTypeID)
      { case SQL_LONGVARCHAR:
	case SQL_VARCHAR:
	{ int qlen = max_qualifier_lenght(ctxt->connection);

	  if ( qlen > 0 )
	  { /*Sdprintf("Using SQL_MAX_QUALIFIER_NAME_LEN = %d\n", qlen);*/
	    ptr_result->len_value = qlen+1; /* play safe */
	    goto bind;
	  } else if ( qlen < 0 )	/* error getting it */
	    return FALSE;
	}
      }
    }

    switch (ptr_result->sqlTypeID)
    { case SQL_LONGVARCHAR:
      case SQL_LONGVARBINARY:
      { if ( columnSize > ctxt->max_nogetdata || columnSize == 0 )
	{ use_sql_get_data:
	  DEBUG(2,
		Sdprintf("Wide SQL_LONGVAR* column %d: using SQLGetData()\n", i));
	  ptr_result->ptr_value = NULL;	/* handle using SQLGetData() */
	  continue;
	}
	ptr_result->len_value = sizeof(char)*(columnSize+1);
	goto bind;
      }
    }

    switch (ptr_result->cTypeID)
    { case SQL_C_CHAR:
	if ( columnSize == 0 )
	  goto use_sql_get_data;
	columnSize++;			/* one for decimal dot */
        /*FALLTHROUGH*/
      case SQL_C_BINARY:
	if ( columnSize > ctxt->max_nogetdata || columnSize == 0 )
	  goto use_sql_get_data;
        ptr_result->len_value = sizeof(char)*columnSize+1;
	break;
      case SQL_C_SLONG:
	ptr_result->len_value = sizeof(SQLINTEGER);
	break;
      case SQL_C_SBIGINT:
	ptr_result->len_value = sizeof(SQLBIGINT);
	break;
      case SQL_C_DOUBLE:
	ptr_result->len_value = sizeof(SQLDOUBLE);
	break;
      case SQL_C_TYPE_DATE:
	ptr_result->len_value = sizeof(DATE_STRUCT);
	break;
      case SQL_C_TYPE_TIME:
	ptr_result->len_value = sizeof(TIME_STRUCT);
	break;
      case SQL_C_TIMESTAMP:
	ptr_result->len_value = sizeof(SQL_TIMESTAMP_STRUCT);
	break;
      default:
	assert(0);
        return FALSE;			/* make compiler happy */
    }

  bind:
    if ( ptr_result->len_value <= PARAM_BUFSIZE )
      ptr_result->ptr_value = (void *)ptr_result->buf;
    else
    { if ( !(ptr_result->ptr_value = odbc_malloc(ptr_result->len_value)) )
	return FALSE;
    }

    TRY(ctxt, SQLBindCol(ctxt->hstmt, i,
			 ptr_result->cTypeID,
			 ptr_result->ptr_value,
			 ptr_result->len_value,
			 &ptr_result->length_ind),
       (void)0);
  }

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
odbc_row()  is  the  final  call  from  the  various  query  predicates,
returning a result row or, in case  of findall, the whole result-set. It
must call close_context() if we are  done   with  the  context due to an
error or the last result.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static foreign_t
odbc_row(context *ctxt, term_t trow)
{ term_t local_trow;
  fid_t fid;

  if ( !true(ctxt, CTX_BOUND) )
  { if ( !prepare_result(ctxt) )
    { close_context(ctxt);
      return FALSE;
    }
    set(ctxt, CTX_BOUND);
  }

  if ( !ctxt->result )			/* not a SELECT statement */
  { SQLLEN rows;			/* was DWORD */
    int rval;

    ctxt->rc = SQLRowCount(ctxt->hstmt, &rows);
    if ( ctxt->rc == SQL_SUCCESS ||
	 ctxt->rc == SQL_SUCCESS_WITH_INFO )
      rval = PL_unify_term(trow,
			   PL_FUNCTOR, FUNCTOR_affected1,
			     PL_LONG, (long)rows);
    else
      rval = TRUE;

    close_context(ctxt);

    return rval;
  }

  if ( ctxt->findall )			/* findall: return the whole set */
  { term_t tail = PL_copy_term_ref(trow);
    term_t head = PL_new_term_ref();
    term_t tmp  = PL_new_term_ref();

    for(;;)
    { ctxt->rc = SQLFetch(ctxt->hstmt);

      switch(ctxt->rc)
      { case SQL_NO_DATA_FOUND:
	  close_context(ctxt);
	  return PL_unify_nil(tail);
	case SQL_SUCCESS:
	  break;
	default:
	  if ( !report_status(ctxt) )
	  { close_context(ctxt);
	    return FALSE;
	  }
      }

      if ( !PL_unify_list(tail, head, tail) ||
	   !put_findall(ctxt, tmp) ||
	   !PL_unify(head, tmp) )
      { close_context(ctxt);
	return FALSE;
      }
    }
  }

  local_trow = PL_new_term_ref();
  fid = PL_open_foreign_frame();

  for(;;)				/* normal non-deterministic access */
  { if ( true(ctxt, CTX_PREFETCHED) )
      clear(ctxt, CTX_PREFETCHED);
    else
      TRY(ctxt, SQLFetch(ctxt->hstmt), close_context(ctxt));

    if ( !pl_put_row(local_trow, ctxt) )
    { close_context(ctxt);
      return FALSE;			/* with pending exception */
    }

    if ( !PL_unify(trow, local_trow) )
    { PL_rewind_foreign_frame(fid);
      continue;
    }

					/* success! */
					/* pre-fetch to get determinism */
    ctxt->rc = SQLFetch(ctxt->hstmt);
    switch(ctxt->rc)
    { case SQL_NO_DATA_FOUND:		/* no alternative */
	close_context(ctxt);
	return TRUE;
      case SQL_SUCCESS_WITH_INFO:
	report_status(ctxt);		/* Always returns TRUE */
        /*FALLTHROUGH*/
      case SQL_SUCCESS:
	set(ctxt, CTX_PREFETCHED);
	PL_retry_address(ctxt);
      default:
	if ( !report_status(ctxt) )
	{ close_context(ctxt);
	  return FALSE;
	}
        return TRUE;
    }
  }
}


static int
set_column_types(context *ctxt, term_t option)
{ term_t tail = PL_new_term_ref();
  term_t head = PL_new_term_ref();
  parameter *p;
  int ntypes;

  PL_get_arg(1, option, tail);
  if ( (ntypes = list_length(tail)) < 0 )
    return FALSE;			/* not a proper list */

  ctxt->NumCols = ntypes;
  ctxt->db_row = PL_new_functor(ATOM_row, ctxt->NumCols);
  if ( !(ctxt->result = odbc_malloc(sizeof(parameter)*ctxt->NumCols)) )
    return FALSE;
  memset(ctxt->result, 0, sizeof(parameter)*ctxt->NumCols);

  for(p = ctxt->result; PL_get_list(tail, head, tail); p++)
  { if ( !get_pltype(head, &p->plTypeID) )
      return FALSE;
  }
  if ( !PL_get_nil(tail) )
    return type_error(tail, "list");

  return TRUE;
}


static int
set_statement_options(context *ctxt, term_t options)
{ if ( !PL_get_nil(options) )
  { term_t tail = PL_copy_term_ref(options);
    term_t head = PL_new_term_ref();

    while(PL_get_list(tail, head, tail))
    { if ( PL_is_functor(head, FUNCTOR_types1) )
      { if ( !set_column_types(ctxt, head) )
	  return FALSE;
      } else if ( PL_is_functor(head, FUNCTOR_null1) )
      { term_t arg = PL_new_term_ref();

	PL_get_arg(1, head, arg);
	ctxt->null = nulldef_spec(arg);
	set(ctxt, CTX_OWNNULL);
      } else if ( PL_is_functor(head, FUNCTOR_source1) )
      { int val;

	if ( !get_bool_arg_ex(1, head, &val) )
	  return FALSE;

	if ( val )
	  set(ctxt, CTX_SOURCE);
      } else if ( PL_is_functor(head, FUNCTOR_findall2) )
      { if ( !(ctxt->findall = compile_findall(head, ctxt->flags)) )
	  return FALSE;
      } else if ( PL_is_functor(head, FUNCTOR_fetch1) )
      { atom_t a;

	if ( !get_atom_arg_ex(1, head, &a) )
	  return FALSE;
	if ( a == ATOM_auto )
	  clear(ctxt, CTX_NOAUTO);
	else if ( a == ATOM_fetch )
	  set(ctxt, CTX_NOAUTO);
	else
	{ term_t a = PL_new_term_ref();
	  PL_get_arg(1, head, a);
	  return domain_error(a, "fetch");
	}
      } else if ( PL_is_functor(head, FUNCTOR_wide_column_threshold1) )
      { int val;

	if ( !get_int_arg_ex(1, head, &val) )
	  return FALSE;

	ctxt->max_nogetdata = val;
      } else
	return domain_error(head, "odbc_option");
    }
    if ( !PL_get_nil(tail) )
      return type_error(tail, "list");
  }

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
odbc_query(+DSN, +SQL, -Row)
    Execute an SQL query, returning the result-rows 1-by-1 on
    backtracking
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static foreign_t
pl_odbc_query(term_t dsn, term_t tquery, term_t trow, term_t options,
	      control_t handle)
{ context *ctxt;

  switch( PL_foreign_control(handle) )
  { case PL_FIRST_CALL:
    { connection *cn;

      if ( !get_connection(dsn, &cn) )
	return FALSE;

      if ( !(ctxt = new_context(cn)) )
	return FALSE;
      if ( !get_sql_text(ctxt, tquery) )
      { free_context(ctxt);
	return FALSE;
      }

      if ( !set_statement_options(ctxt, options) )
      { free_context(ctxt);
	return FALSE;
      }
      set(ctxt, CTX_INUSE);
      TRY(ctxt,
	  SQLExecDirect(ctxt->hstmt, (SQLCHAR*)ctxt->sqltext, ctxt->sqllen),
	  close_context(ctxt));

      return odbc_row(ctxt, trow);
    }
    case PL_REDO:
      return odbc_row(PL_foreign_context_address(handle), trow);

    default:
    case PL_CUTTED:
      free_context(PL_foreign_context_address(handle));
      return TRUE;
  }
}


		 /*******************************
		 *	DICTIONARY SUPPORT	*
		 *******************************/

static foreign_t
odbc_tables(term_t dsn, term_t row, control_t handle)
{ switch( PL_foreign_control(handle) )
  { case PL_FIRST_CALL:
    { connection *cn;
      context *ctxt;

      if ( !get_connection(dsn, &cn) )
	return FALSE;

      if ( !(ctxt = new_context(cn)) )
	return FALSE;
      ctxt->null = NULL;		/* use default $null$ */
      set(ctxt, CTX_TABLES);
      TRY(ctxt,
	  SQLTables(ctxt->hstmt, NULL,0,NULL,0,NULL,0,NULL,0),
	  close_context(ctxt));

      return odbc_row(ctxt, row);
    }
    case PL_REDO:
      return odbc_row(PL_foreign_context_address(handle), row);

    case PL_CUTTED:
      free_context(PL_foreign_context_address(handle));
      return TRUE;

    default:
      assert(0);
      return FALSE;
  }
}


static foreign_t
pl_odbc_column(term_t dsn, term_t db, term_t row, control_t handle)
{ switch( PL_foreign_control(handle) )
  { case PL_FIRST_CALL:
    { connection *cn;
      context *ctxt;
      size_t len;
      char *s;

      if ( !PL_get_nchars(db, &len, &s, CVT_ATOM|CVT_STRING) )
	return type_error(db, "atom");

      if ( !get_connection(dsn, &cn) )
	return FALSE;
      if ( !(ctxt = new_context(cn)) )
	return FALSE;
      ctxt->null = NULL;		/* use default $null$ */
      set(ctxt, CTX_COLUMNS);
      TRY(ctxt,
	  SQLColumns(ctxt->hstmt, NULL, 0, NULL, 0,
		     (SQLCHAR*)s, (SWORD)len, NULL, 0),
	  close_context(ctxt));

      return odbc_row(ctxt, row);
    }
    case PL_REDO:
      return odbc_row(PL_foreign_context_address(handle), row);

    case PL_CUTTED:
      free_context(PL_foreign_context_address(handle));
      return TRUE;

    default:
      assert(0);
      return FALSE;
  }
}


static foreign_t
odbc_types(term_t dsn, term_t sqltype, term_t row, control_t handle)
{ switch( PL_foreign_control(handle) )
  { case PL_FIRST_CALL:
    { connection *cn;
      context *ctxt;
      atom_t tname;
      SWORD type;
      int v;

      if ( PL_get_integer(sqltype, &v) )
      { type = v;
      } else
      { if ( !PL_get_atom(sqltype, &tname) )
	  return type_error(sqltype, "sql_type");
	if ( tname == ATOM_all_types )
	  type = SQL_ALL_TYPES;
	else if ( !get_sqltype_from_atom(tname, &type) )
	  return domain_error(sqltype, "sql_type");
      }

      if ( !get_connection(dsn, &cn) )
	return FALSE;
      if ( !(ctxt = new_context(cn)) )
	return FALSE;
      ctxt->null = NULL;		/* use default $null$ */
      TRY(ctxt,
	  SQLGetTypeInfo(ctxt->hstmt, type),
	  close_context(ctxt));

      return odbc_row(ctxt, row);
    }
    case PL_REDO:
      return odbc_row(PL_foreign_context_address(handle), row);

    case PL_CUTTED:
      free_context(PL_foreign_context_address(handle));
      return TRUE;

    default:
      assert(0);
      return FALSE;
  }
}


static foreign_t
odbc_data_sources(term_t list)
{ UCHAR dsn[SQL_MAX_DSN_LENGTH];
  UCHAR description[1024];
  SWORD dsnlen, dlen;
  UWORD dir = SQL_FETCH_FIRST;
  RETCODE rc;
  term_t tail = PL_copy_term_ref(list);
  term_t head = PL_new_term_ref();

  if ( !henv )
    SQLAllocEnv(&henv);		/* Allocate an environment handle */

  for(;; dir=SQL_FETCH_NEXT)
  { rc = SQLDataSources(henv,
			dir,
			dsn, sizeof(dsn)-1, &dsnlen,
			description, sizeof(description)-1, &dlen);
    switch(rc)
    { case SQL_SUCCESS:
      { if ( PL_unify_list(tail, head, tail) &&
	     PL_unify_term(head, PL_FUNCTOR, FUNCTOR_data_source2,
			           PL_NCHARS, dsnlen, dsn,
			           PL_NCHARS, dlen, description) )
	  continue;

	return FALSE;
      }
      case SQL_NO_DATA_FOUND:
	return PL_unify_nil(tail);
      default:
	odbc_report(henv, NULL, NULL, rc);
        return FALSE;
    }
  }
}


		 /*******************************
		 *	COMPILE STATEMENTS	*
		 *******************************/

static int
unifyStmt(term_t id, context *ctxt)
{ return PL_unify_term(id, PL_FUNCTOR, FUNCTOR_odbc_statement1,
		             PL_POINTER, ctxt);
}


static int
getStmt(term_t id, context **ctxt)
{ if ( PL_is_functor(id, FUNCTOR_odbc_statement1) )
  { term_t a = PL_new_term_ref();
    void *ptr;

    PL_get_arg(1, id, a);
    if ( PL_get_pointer(a, &ptr) )
    { *ctxt = ptr;

      if ( (*ctxt)->magic != CTX_MAGIC )
	return existence_error(id, "odbc_statement_handle");

      return TRUE;
    }
  }

  return type_error(id, "odbc_statement_handle");
}


typedef struct
{ SWORD  type;				/* SQL_* */
  const char *text;			/* same as text */
  atom_t name;				/* Prolog name */
} sqltypedef;

static sqltypedef sqltypes[] =
{ { SQL_BIGINT,	       "bigint" },
  { SQL_BINARY,	       "binary" },
  { SQL_BIT,	       "bit" },
  { SQL_CHAR,	       "char" },
  { SQL_DATE,	       "date" },
  { SQL_DECIMAL,       "decimal" },
  { SQL_DOUBLE,	       "double" },
  { SQL_FLOAT,	       "float" },
  { SQL_INTEGER,       "integer" },
  { SQL_LONGVARBINARY, "longvarbinary" },
  { SQL_LONGVARCHAR,   "longvarchar" },
  { SQL_NUMERIC,       "numeric" },
  { SQL_REAL,	       "real" },
  { SQL_SMALLINT,      "smallint" },
  { SQL_TIME,	       "time" },
  { SQL_TIMESTAMP,     "timestamp" },
  { SQL_TINYINT,       "tinyint" },
  { SQL_VARBINARY,     "varbinary" },
  { SQL_VARCHAR,       "varchar" },
  { 0,		       NULL }
};


static SWORD
get_sqltype_from_atom(atom_t name, SWORD *type)
{ sqltypedef *def;

  for(def=sqltypes; def->text; def++)
  { if ( !def->name )
      def->name = PL_new_atom(def->text);
    if ( def->name == name )
    { *type = def->type;
      return TRUE;
    }
  }

  return FALSE;
}

static sqltypedef pltypes[] =
{ { SQL_PL_DEFAULT,    "default" },
  { SQL_PL_ATOM,       "atom" },
  { SQL_PL_STRING,     "string" },
  { SQL_PL_CODES,      "codes" },
  { SQL_PL_INTEGER,    "integer" },
  { SQL_PL_FLOAT,      "float" },
  { SQL_PL_TIME,       "time" },
  { SQL_PL_DATE,       "date" },
  { SQL_PL_TIMESTAMP,  "timestamp" },
  { 0,		       NULL }
};


static int
get_pltype(term_t t, SWORD *type)
{ atom_t name;

  if ( PL_get_atom(t, &name) )
  { sqltypedef *def;

    for(def=pltypes; def->text; def++)
    { if ( !def->name )
	def->name = PL_new_atom(def->text);

      if ( def->name == name )
      { *type = def->type;
        return TRUE;
      }
    }

    return domain_error(t, "sql_prolog_type");
  }

  return type_error(t, "atom");
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Declare parameters for prepared statements.

odbc_prepare(DSN, 'select * from product where price < ?',
	     [ integer
	     ],
	     Qid,
	     Options).
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
declare_parameters(context *ctxt, term_t parms)
{ int nparams;
  term_t tail = PL_copy_term_ref(parms);
  term_t head = PL_new_term_ref();
  parameter *params;
  SWORD npar;
  int pn;

  TRY(ctxt,
      SQLNumParams(ctxt->hstmt, &npar),
      (void)0);
  if ( (nparams=list_length(parms)) < 0 )
    return FALSE;
  if ( npar != nparams )
    return domain_error(parms, "length"); /* TBD: What error to raise?? */

  ctxt->NumParams = nparams;
  if ( nparams == 0 )
    return TRUE;			/* no parameters */

  if ( !(ctxt->params = odbc_malloc(sizeof(parameter)*nparams)) )
    return FALSE;
  memset(ctxt->params, 0, sizeof(parameter)*nparams);
  params = ctxt->params;

  for(params = ctxt->params, pn = 1;
      PL_get_list(tail, head, tail);
      params++, pn++)
  { atom_t name;
    int arity;
    SWORD sqlType, fNullable;
    SQLULEN cbColDef = 0;
    SWORD plType = SQL_PL_DEFAULT;
    SQLLEN *vlenptr = NULL;		/* pointer to length */

    if ( PL_is_functor(head, FUNCTOR_gt2) )
    { term_t a = PL_new_term_ref();

      PL_get_arg(1, head, a);
      if ( !get_pltype(a, &plType) )
	return FALSE;

      PL_get_arg(2, head, head);
    }

    if ( !PL_get_name_arity(head, &name, &arity) )
      return type_error(head, "parameter_type");

    if ( name != ATOM_default )
    { int val;

      if ( !get_sqltype_from_atom(name, &sqlType) )
	return domain_error(head, "parameter_type");

					/* char(N) --> cbColDef */
      if ( get_int_arg(1, head, &val) )	/* TBD: incomplete */
	cbColDef = val;
      if ( get_int_arg(2, head, &val) )	/* decimal(cbColDef, scale) */
	params->scale = val;
    } else
    { TRY(ctxt, SQLDescribeParam(ctxt->hstmt,		/* hstmt */
				 (SWORD)pn,		/* ipar */
				 &sqlType,
				 &cbColDef,
				 &params->scale,
				 &fNullable),
	  (void)0);
    }

    params->sqlTypeID = sqlType;
    params->plTypeID  = plType;
    params->cTypeID   = CvtSqlToCType(ctxt, params->sqlTypeID, plType);
    params->ptr_value = (SQLPOINTER)params->buf;

    switch(params->cTypeID)
    { case SQL_C_CHAR:
      case SQL_C_BINARY:
	if ( cbColDef > 0 )
	{ if ( params->sqlTypeID == SQL_DECIMAL ||
	       params->sqlTypeID == SQL_NUMERIC )
	  { cbColDef++;			/* add one for decimal dot */
	    /*Sdprintf("cbColDef++ = %d\n", cbColDef);*/
	  }
	  if ( cbColDef+1 > PARAM_BUFSIZE )
	  { if ( !(params->ptr_value = odbc_malloc(cbColDef+1)) )
	      return FALSE;
	  }
	  params->length_ind = cbColDef;
	} else				/* unknown, use SQLPutData() */
	{ params->ptr_value = (PTR)(long)pn;
	  params->len_value = SQL_LEN_DATA_AT_EXEC(0);
	  DEBUG(2, Sdprintf("Using SQLPutData() for column %d\n", pn));
	}
        vlenptr = &params->len_value;
	break;
      case SQL_C_SLONG:
	params->len_value = sizeof(SQLINTEGER);
        vlenptr = &params->len_value;
	break;
      case SQL_C_SBIGINT:
	params->len_value = sizeof(SQLBIGINT);
        vlenptr = &params->len_value;
	break;
      case SQL_C_DOUBLE:
	params->len_value = sizeof(SQLDOUBLE);
        vlenptr = &params->len_value;
	break;
      case SQL_C_DATE:
      case SQL_C_TYPE_DATE:
	if ( !(params->ptr_value = odbc_malloc(sizeof(DATE_STRUCT))) )
	  return FALSE;
        params->len_value = sizeof(DATE_STRUCT);
	vlenptr = &params->len_value;
        break;
      case SQL_C_TIME:
      case SQL_C_TYPE_TIME:
	if ( !(params->ptr_value = odbc_malloc(sizeof(TIME_STRUCT))) )
	  return FALSE;
        params->len_value = sizeof(TIME_STRUCT);
	vlenptr = &params->len_value;
        break;
      case SQL_C_TIMESTAMP:
	if ( !(params->ptr_value = odbc_malloc(sizeof(SQL_TIMESTAMP_STRUCT))) )
	  return FALSE;
        params->len_value = sizeof(SQL_TIMESTAMP_STRUCT);
	vlenptr = &params->len_value;
        break;
      default:
	Sdprintf("declare_parameters(): cTypeID %d not supported\n",
		 params->cTypeID);
    }


    TRY(ctxt, SQLBindParameter(ctxt->hstmt,		/* hstmt */
			       (SWORD)pn,		/* ipar */
			       SQL_PARAM_INPUT,		/* fParamType */
			       params->cTypeID,		/* fCType */
			       params->sqlTypeID,	/* fSqlType */
			       params->length_ind,	/* cbColDef */
			       params->scale,		/* ibScale */
			       params->ptr_value,	/* rgbValue */
			       0,			/* cbValueMax */
			       vlenptr), 		/* pcbValue */
	(void)0);
  }

  return TRUE;
}


static foreign_t
odbc_prepare(term_t dsn, term_t sql, term_t parms, term_t qid, term_t options)
{ connection *cn;
  context *ctxt;

  if ( !get_connection(dsn, &cn) )
    return FALSE;

  if ( !(ctxt = new_context(cn)) )
    return FALSE;
  if ( !get_sql_text(ctxt, sql) )
  { free_context(ctxt);
    return FALSE;
  }

  TRY(ctxt,
      SQLPrepare(ctxt->hstmt, (SQLCHAR*)ctxt->sqltext, ctxt->sqllen),
      close_context(ctxt));

  if ( !declare_parameters(ctxt, parms) )
  { free_context(ctxt);
    return FALSE;
  }

  ctxt->flags |= CTX_PERSISTENT;

  if ( !set_statement_options(ctxt, options) )
  { free_context(ctxt);
    return FALSE;
  }

  return unifyStmt(qid, ctxt);
}


static foreign_t
odbc_clone_statement(term_t qid, term_t cloneqid)
{ context *ctxt, *clone;

  if ( !getStmt(qid, &ctxt) )
    return FALSE;
  if ( !(clone = clone_context(ctxt)) )
    return FALSE;

  clone->flags |= CTX_PERSISTENT;

  return unifyStmt(cloneqid, clone);
}


static foreign_t
odbc_free_statement(term_t qid)
{ context *ctxt;

  if ( !getStmt(qid, &ctxt) )
    return FALSE;

  if ( true(ctxt, CTX_INUSE) )
    clear(ctxt, CTX_PERSISTENT);	/* oops, delay! */
  else
    free_context(ctxt);

  return TRUE;
}


static int
get_date(term_t head, DATE_STRUCT* date)
{ if ( PL_is_functor(head, FUNCTOR_date3) )
  { int v;

    if ( !get_int_arg(1, head, &v) ) return FALSE;
    date->year = v;
    if ( !get_int_arg(2, head, &v) ) return FALSE;
    date->month = v;
    if ( !get_int_arg(3, head, &v) ) return FALSE;
    date->day = v;

    return TRUE;
  }

  return FALSE;
}


static int
get_time(term_t head, TIME_STRUCT* time)
{ if ( PL_is_functor(head, FUNCTOR_time3) )
  { int v;

    if ( !get_int_arg(1, head, &v) ) return FALSE;
    time->hour = v;
    if ( !get_int_arg(2, head, &v) ) return FALSE;
    time->minute = v;
    if ( !get_int_arg(3, head, &v) ) return FALSE;
    time->second = v;

    return TRUE;
  }

  return FALSE;
}


static int
get_timestamp(term_t t, SQL_TIMESTAMP_STRUCT* stamp)
{
#if defined(HAVE_LOCALTIME) || defined(HAVE_GMTIME)
  double tf;
#endif

  if ( PL_is_functor(t, FUNCTOR_timestamp7) )
  { int v;

    if ( !get_int_arg(1, t, &v) ) return FALSE;
    stamp->year = v;
    if ( !get_int_arg(2, t, &v) ) return FALSE;
    stamp->month = v;
    if ( !get_int_arg(3, t, &v) ) return FALSE;
    stamp->day = v;
    if ( !get_int_arg(4, t, &v) ) return FALSE;
    stamp->hour = v;
    if ( !get_int_arg(5, t, &v) ) return FALSE;
    stamp->minute = v;
    if ( !get_int_arg(6, t, &v) ) return FALSE;
    stamp->second = v;
    if ( !get_int_arg(7, t, &v) ) return FALSE;
    stamp->fraction = v;

    return TRUE;
#if defined(HAVE_LOCALTIME) || defined(HAVE_GMTIME)
  } else if ( PL_get_float(t, &tf) && tf <= LONG_MAX && tf >= LONG_MIN )
  { time_t t = (time_t) tf;
    long  us = (long)((tf - (double) t) * 1000.0);
#if defined(HAVE_GMTIME) && defined USE_UTC
    struct tm *tm = gmtime(&t);
#else
    struct tm *tm = localtime(&t);
#endif

    stamp->year	    = tm->tm_year + 1900;
    stamp->month    = tm->tm_mon + 1;
    stamp->day	    = tm->tm_mday;
    stamp->hour	    = tm->tm_hour;
    stamp->minute   = tm->tm_min;
    stamp->second   = tm->tm_sec;
    stamp->fraction = us;

    return TRUE;
#endif
  } else
    return FALSE;
}


static int
try_null(context *ctxt, parameter *prm, term_t val, const char *expected)
{ if ( is_sql_null(val, ctxt->null) )
  { prm->len_value = SQL_NULL_DATA;

    return TRUE;
  } else
    return type_error(val, expected);
}


static int
get_parameter_text(term_t t, parameter *prm, size_t *len, char **s)
{ unsigned int flags = CVT_ATOM|CVT_STRING;
  const char *expected = "text";

  switch(prm->plTypeID)
  { case SQL_PL_DEFAULT:
      flags = CVT_ATOM|CVT_STRING;
      expected = "text";
      break;
    case SQL_PL_ATOM:
      flags = CVT_ATOM;
      expected = "atom";
      break;
    case SQL_PL_STRING:
      flags = CVT_STRING;
      expected = "string";
      break;
    case SQL_PL_CODES:
      flags = CVT_LIST;
      expected = "code_list";
      break;
    default:
      assert(0);
  }

  if ( !PL_get_nchars(t, len, s, flags) )
    return type_error(t, expected);

  return TRUE;
}



static int
bind_parameters(context *ctxt, term_t parms)
{ term_t tail = PL_copy_term_ref(parms);
  term_t head = PL_new_term_ref();
  parameter *prm;

  for(prm = ctxt->params; PL_get_list(tail, head, tail); prm++)
  { if ( prm->len_value == SQL_LEN_DATA_AT_EXEC(0) )
    { DEBUG(2, Sdprintf("bind_parameters(): Delaying column %d\n",
		     prm-ctxt->params+1));
      prm->put_data = PL_copy_term_ref(head);
      continue;
    }

    switch(prm->cTypeID)
    { case SQL_C_SLONG:
      { long val;

	if ( PL_get_long(head, &val) )
	{ SQLINTEGER sqlval = val;
	  memcpy(prm->ptr_value, &sqlval, sizeof(SQLINTEGER));
	  prm->len_value = sizeof(SQLINTEGER);
	} else if ( !try_null(ctxt, prm, head, "integer") )
	  return FALSE;
        break;
      }
      case SQL_C_SBIGINT:
      { int64_t val;

	if ( PL_get_int64(head, &val) )
	{ SQLBIGINT sqlval = val;
	  memcpy(prm->ptr_value, &sqlval, sizeof(SQLBIGINT));
	  prm->len_value = sizeof(SQLBIGINT);
	} else if ( !try_null(ctxt, prm, head, "integer") )
	  return FALSE;
        break;
      }
      case SQL_C_DOUBLE:
	if ( PL_get_float(head, (double *)prm->ptr_value) )
	  prm->len_value = sizeof(double);
        else if ( !try_null(ctxt, prm, head, "float") )
	  return FALSE;
        break;
      case SQL_C_CHAR:
      case SQL_C_BINARY:
      { SQLLEN len;
	size_t l;
	char *s;

					/* check for NULL */
	if ( is_sql_null(head, ctxt->null) )
	{ prm->len_value = SQL_NULL_DATA;
	  break;
	}

	if ( !get_parameter_text(head, prm, &l, &s) )
	  return FALSE;
	len = l;

	if ( len > prm->length_ind )
	{ DEBUG(1, Sdprintf("Column-width = %d\n", prm->length_ind));
	  return representation_error(head, "column_width");
	}
	memcpy(prm->ptr_value, s, len+1);
	prm->len_value = len;
	break;
      }
      case SQL_C_TYPE_DATE:
      { if ( get_date(head, (DATE_STRUCT*)prm->ptr_value) )
	  prm->len_value = sizeof(DATE_STRUCT);
	else if ( !try_null(ctxt, prm, head, "date") )
	  return FALSE;
	break;
      }
      case SQL_C_TYPE_TIME:
      { if ( get_time(head, (TIME_STRUCT*)prm->ptr_value) )
	  prm->len_value = sizeof(TIME_STRUCT);
	else if ( !try_null(ctxt, prm, head, "time") )
	  return FALSE;
	break;
      }
      case SQL_C_TIMESTAMP:
      { if ( get_timestamp(head, (SQL_TIMESTAMP_STRUCT*)prm->ptr_value) )
	  prm->len_value = sizeof(SQL_TIMESTAMP_STRUCT);
	else if ( !try_null(ctxt, prm, head, "timestamp") )
	  return FALSE;
	break;
      }
      default:
	return PL_warning("Unknown parameter type: %d", prm->cTypeID);
    }
  }
  if ( !PL_get_nil(tail) )
    return type_error(tail, "list");

  return TRUE;
}

static foreign_t
odbc_execute(term_t qid, term_t args, term_t row, control_t handle)
{ switch( PL_foreign_control(handle) )
  { case PL_FIRST_CALL:
    { context *ctxt;

      if ( !getStmt(qid, &ctxt) )
	return FALSE;
      if ( true(ctxt, CTX_INUSE) )
      { context *clone;

	if ( true(ctxt, CTX_NOAUTO) || !(clone = clone_context(ctxt)) )
	  return context_error(qid, "in_use", "statement");
	else
	  ctxt = clone;
      }

      if ( !bind_parameters(ctxt, args) )
	return FALSE;

      set(ctxt, CTX_INUSE);
      clear(ctxt, CTX_PREFETCHED);

      ctxt->rc = SQLExecute(ctxt->hstmt);
      while( ctxt->rc == SQL_NEED_DATA )
      { PTR token;

	if ( (ctxt->rc = SQLParamData(ctxt->hstmt, &token)) == SQL_NEED_DATA )
	{ parameter *p = &ctxt->params[(long)token - 1];
	  size_t len;
	  char *s;

	  if ( is_sql_null(p->put_data, ctxt->null) )
	  { s = NULL;
	    len = SQL_NULL_DATA;
	  } else
	  { if ( !get_parameter_text(p->put_data, p, &len, &s) )
	      return FALSE;
	  }
	  SQLPutData(ctxt->hstmt, s, len);
	}
      }
      if ( !report_status(ctxt) )
      { close_context(ctxt);
	return FALSE;
      }

      if ( true(ctxt, CTX_NOAUTO) )
	return TRUE;

      return odbc_row(ctxt, row);
    }

    case PL_REDO:
      return odbc_row(PL_foreign_context_address(handle), row);

    case PL_CUTTED:
      close_context(PL_foreign_context_address(handle));
      return TRUE;

    default:
      assert(0);
      return FALSE;
  }
}


static int
get_scroll_param(term_t param, int *orientation, long *offset)
{ atom_t name;
  int arity;

  if ( PL_get_name_arity(param, &name, &arity) )
  { if ( name == ATOM_next && arity == 0 )
    { *orientation = SQL_FETCH_NEXT;
      *offset = 0;
      return TRUE;
    } else if ( name == ATOM_prior && arity == 0 )
    { *orientation = SQL_FETCH_PRIOR;
      *offset = 0;
      return TRUE;
    } else if ( name == ATOM_first && arity == 0 )
    { *orientation = SQL_FETCH_FIRST;
      *offset = 0;
      return TRUE;
    } else if ( name == ATOM_last && arity == 0 )
    { *orientation = SQL_FETCH_LAST;
      *offset = 0;
      return TRUE;
    } else if ( name == ATOM_absolute && arity == 1 )
    { *orientation = SQL_FETCH_ABSOLUTE;
      return get_long_arg_ex(1, param, offset);
    } else if ( name == ATOM_relative && arity == 1 )
    { *orientation = SQL_FETCH_RELATIVE;
      return get_long_arg_ex(1, param, offset);
    } else if ( name == ATOM_bookmark && arity == 1 )
    { *orientation = SQL_FETCH_BOOKMARK;
      return get_long_arg_ex(1, param, offset);
    } else
      return domain_error(param, "fetch_option");
  }

  return type_error(param, "fetch_option");
}



static foreign_t
odbc_fetch(term_t qid, term_t row, term_t options)
{ context *ctxt;
  term_t local_trow = PL_new_term_ref();
  int orientation;
  long offset;

  if ( !getStmt(qid, &ctxt) )
    return FALSE;
  if ( false(ctxt, CTX_NOAUTO) || false(ctxt, CTX_INUSE) )
    return permission_error("fetch", "statement", qid);

  if ( !true(ctxt, CTX_BOUND) )
  { if ( !prepare_result(ctxt) )
      return FALSE;
    set(ctxt, CTX_BOUND);
  }

  if ( PL_get_nil(options) )
  { orientation = SQL_FETCH_NEXT;
  } else if ( PL_is_list(options) )
  { term_t tail = PL_copy_term_ref(options);
    term_t head = PL_new_term_ref();

    while(PL_get_list(tail, head, tail))
    { if ( !get_scroll_param(head, &orientation, &offset) )
	return FALSE;
    }
    if ( !PL_get_nil(tail) )
      return type_error(tail, "list");
  } else if ( !get_scroll_param(options, &orientation, &offset) )
    return FALSE;

  if ( orientation == SQL_FETCH_NEXT )
    ctxt->rc = SQLFetch(ctxt->hstmt);
  else
    ctxt->rc = SQLFetchScroll(ctxt->hstmt,
			      (SQLSMALLINT)orientation,
			      (SQLINTEGER)offset);

  switch(ctxt->rc)
  { case SQL_NO_DATA_FOUND:		/* no alternative */
      close_context(ctxt);
      return PL_unify_atom(row, ATOM_end_of_file);
    case SQL_SUCCESS_WITH_INFO:
      report_status(ctxt);
      /*FALLTHROUGH*/
    case SQL_SUCCESS:
      if ( !pl_put_row(local_trow, ctxt) )
      { close_context(ctxt);
	return FALSE;			/* with pending exception */
      }

      return PL_unify(local_trow, row);
    default:
      if ( !report_status(ctxt) )
      { close_context(ctxt);
	return FALSE;
      }

      return TRUE;
  }
}


static foreign_t
odbc_close_statement(term_t qid)
{ context *ctxt;

  if ( !getStmt(qid, &ctxt) )
    return FALSE;

  close_context(ctxt);

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
$odbc_statistics/1

	NOTE: enumeration of available statistics is done in Prolog
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static functor_t FUNCTOR_statements2;	/* statements(created,freed) */

static int
unify_int_arg(int pos, term_t t, long val)
{ term_t a = PL_new_term_ref();

  if ( PL_get_arg(pos, t, a) )
    return PL_unify_integer(a, val);

  return FALSE;
}


static foreign_t
odbc_statistics(term_t what)
{ if ( !PL_is_compound(what) )
    return type_error(what, "compound");

  if ( PL_is_functor(what, FUNCTOR_statements2) )
  { if ( unify_int_arg(1, what, statistics.statements_created) &&
	 unify_int_arg(2, what, statistics.statements_freed) )
      return TRUE;
  } else
    return domain_error(what, "odbc_statistics");

  return FALSE;
}


static foreign_t
odbc_debug(term_t level)
{ if ( !PL_get_integer(level, &odbc_debuglevel) )
    return type_error(level, "integer");

  return TRUE;
}


#define MKFUNCTOR(name, arity) PL_new_functor(PL_new_atom(name), arity)
#define NDET(name, arity, func) PL_register_foreign(name, arity, func, \
						    PL_FA_NONDETERMINISTIC)
#define DET(name, arity, func) PL_register_foreign(name, arity, func, 0)

install_t
install_odbc4pl()
{  ATOM_row	      =	PL_new_atom("row");
   ATOM_informational =	PL_new_atom("informational");
   ATOM_default	      =	PL_new_atom("default");
   ATOM_once	      =	PL_new_atom("once");
   ATOM_multiple      =	PL_new_atom("multiple");
   ATOM_commit	      =	PL_new_atom("commit");
   ATOM_rollback      =	PL_new_atom("rollback");
   ATOM_atom	      =	PL_new_atom("atom");
   ATOM_string	      =	PL_new_atom("string");
   ATOM_codes	      =	PL_new_atom("codes");
   ATOM_integer	      =	PL_new_atom("integer");
   ATOM_float	      =	PL_new_atom("float");
   ATOM_time	      =	PL_new_atom("time");
   ATOM_date	      =	PL_new_atom("date");
   ATOM_timestamp     =	PL_new_atom("timestamp");
   ATOM_all_types     = PL_new_atom("all_types");
   ATOM_null          = PL_new_atom("$null$");
   ATOM_	      = PL_new_atom("");
   ATOM_read	      = PL_new_atom("read");
   ATOM_update	      = PL_new_atom("update");
   ATOM_dynamic	      = PL_new_atom("dynamic");
   ATOM_forwards_only = PL_new_atom("forwards_only");
   ATOM_keyset_driven = PL_new_atom("keyset_driven");
   ATOM_static	      = PL_new_atom("static");
   ATOM_auto	      = PL_new_atom("auto");
   ATOM_fetch	      = PL_new_atom("fetch");
   ATOM_end_of_file   = PL_new_atom("end_of_file");
   ATOM_next          = PL_new_atom("next");
   ATOM_prior         = PL_new_atom("prior");
   ATOM_first         = PL_new_atom("first");
   ATOM_last          = PL_new_atom("last");
   ATOM_absolute      = PL_new_atom("absolute");
   ATOM_relative      = PL_new_atom("relative");
   ATOM_bookmark      = PL_new_atom("bookmark");

   FUNCTOR_timestamp7		 = MKFUNCTOR("timestamp", 7);
   FUNCTOR_time3		 = MKFUNCTOR("time", 3);
   FUNCTOR_date3		 = MKFUNCTOR("date", 3);
   FUNCTOR_odbc3		 = MKFUNCTOR("odbc", 3);
   FUNCTOR_error2		 = MKFUNCTOR("error", 2);
   FUNCTOR_type_error2		 = MKFUNCTOR("type_error", 2);
   FUNCTOR_domain_error2	 = MKFUNCTOR("domain_error", 2);
   FUNCTOR_existence_error2	 = MKFUNCTOR("existence_error", 2);
   FUNCTOR_resource_error1	 = MKFUNCTOR("resource_error", 1);
   FUNCTOR_permission_error3	 = MKFUNCTOR("permission_error", 3);
   FUNCTOR_representation_error1 = MKFUNCTOR("representation_error", 1);
   FUNCTOR_odbc_statement1	 = MKFUNCTOR("$odbc_statement", 1);
   FUNCTOR_odbc_connection1	 = MKFUNCTOR("$odbc_connection", 1);
   FUNCTOR_user1		 = MKFUNCTOR("user", 1);
   FUNCTOR_password1		 = MKFUNCTOR("password", 1);
   FUNCTOR_driver_string1		 = MKFUNCTOR("driver_string", 1);
   FUNCTOR_alias1		 = MKFUNCTOR("alias", 1);
   FUNCTOR_mars1		 = MKFUNCTOR("mars", 1);
   FUNCTOR_open1		 = MKFUNCTOR("open", 1);
   FUNCTOR_auto_commit1		 = MKFUNCTOR("auto_commit", 1);
   FUNCTOR_types1		 = MKFUNCTOR("types", 1);
   FUNCTOR_minus2		 = MKFUNCTOR("-", 2);
   FUNCTOR_gt2			 = MKFUNCTOR(">", 2);
   FUNCTOR_context_error3	 = MKFUNCTOR("context_error", 3);
   FUNCTOR_statements2		 = MKFUNCTOR("statements", 2);
   FUNCTOR_data_source2		 = MKFUNCTOR("data_source", 2);
   FUNCTOR_null1		 = MKFUNCTOR("null", 1);
   FUNCTOR_source1		 = MKFUNCTOR("source", 1);
   FUNCTOR_column3		 = MKFUNCTOR("column", 3);
   FUNCTOR_access_mode1		 = MKFUNCTOR("access_mode", 1);
   FUNCTOR_cursor_type1		 = MKFUNCTOR("cursor_type", 1);
   FUNCTOR_silent1		 = MKFUNCTOR("silent", 1);
   FUNCTOR_findall2		 = MKFUNCTOR("findall", 2);
   FUNCTOR_affected1		 = MKFUNCTOR("affected", 1);
   FUNCTOR_fetch1		 = MKFUNCTOR("fetch", 1);
   FUNCTOR_wide_column_threshold1= MKFUNCTOR("wide_column_threshold", 1);

   DET("odbc_connect",		   3, pl_odbc_connect);
   DET("odbc_disconnect",	   1, pl_odbc_disconnect);
   NDET("odbc_current_connection", 2, odbc_current_connection);
   DET("odbc_set_connection",	   2, pl_odbc_set_connection);
   NDET("odbc_get_connection",	   2, odbc_get_connection);
   DET("odbc_end_transaction",	   2, odbc_end_transaction);

   DET("odbc_prepare",		   5, odbc_prepare);
   DET("odbc_clone_statement",	   2, odbc_clone_statement);
   DET("odbc_free_statement",	   1, odbc_free_statement);
   NDET("odbc_execute",		   3, odbc_execute);
   DET("odbc_fetch",		   3, odbc_fetch);
   DET("odbc_close_statement",	   1, odbc_close_statement);

   NDET("odbc_query",		   4, pl_odbc_query);
   NDET("odbc_tables",	           2, odbc_tables);
   NDET("odbc_column",		   3, pl_odbc_column);
   NDET("odbc_types",		   3, odbc_types);
   DET("odbc_data_sources",	   1, odbc_data_sources);

   DET("$odbc_statistics",	   1, odbc_statistics);
   DET("odbc_debug",	           1, odbc_debug);
}


install_t
uninstall_odbc()			/* TBD: make sure the library is */
{ if ( henv )				/* not in use! */
  { SQLFreeEnv(henv);
    henv = NULL;
  }
}


		 /*******************************
		 *	      TYPES		*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
MS SQL Server seems to store the   dictionary  in UNICODE, returning the
types SQL_WCHAR, etc. As current SWI-Prolog   doesn't do unicode, we ask
them as normal SQL_C_CHAR, assuming the   driver  will convert this. One
day this should become SQL_C_WCHAR
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static SWORD
CvtSqlToCType(context *ctxt, SQLSMALLINT fSqlType, SQLSMALLINT plTypeID)
{ switch(plTypeID)
  { case SQL_PL_DEFAULT:
      switch (fSqlType)
      { case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
#ifdef SQL_WCHAR
	case SQL_WCHAR:			/* see note above */
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
#endif
	  return SQL_C_CHAR;

	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
	  return SQL_C_BINARY;

	case SQL_DECIMAL:
	case SQL_NUMERIC:
	  return SQL_C_CHAR;

	case SQL_REAL:
	case SQL_FLOAT:
	case SQL_DOUBLE:
	  return SQL_C_DOUBLE;

	case SQL_BIT:
	case SQL_TINYINT:
	case SQL_SMALLINT:
	case SQL_INTEGER:
	  return SQL_C_SLONG;

	case SQL_BIGINT:		/* 64-bit integers */
	  return SQL_C_SBIGINT;

	case SQL_DATE:
	case SQL_TYPE_DATE:
	  return SQL_C_TYPE_DATE;
	case SQL_TIME:
	case SQL_TYPE_TIME:
	  return SQL_C_TYPE_TIME;
	case SQL_TIMESTAMP:
	  return SQL_C_TIMESTAMP;
	default:
	  if ( !true(ctxt, CTX_SILENT) )
	    Sdprintf("Mapped unknown fSqlType %d to atom\n", fSqlType);
	  return SQL_C_CHAR;
      }
    case SQL_PL_ATOM:
    case SQL_PL_STRING:
    case SQL_PL_CODES:
      switch (fSqlType)
      { case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
	  return SQL_C_BINARY;

	default:
	  return SQL_C_CHAR;
      }
    case SQL_PL_INTEGER:
      switch(fSqlType)
      { case SQL_TIMESTAMP:
	  return SQL_C_TIMESTAMP;
	case SQL_BIGINT:		/* 64-bit integers */
	  return SQL_C_SBIGINT;
	default:
	  return SQL_C_SLONG;
      }
    case SQL_PL_FLOAT:
      switch(fSqlType)
      { case SQL_TIMESTAMP:
	  return SQL_C_TIMESTAMP;
	default:
	  return SQL_C_DOUBLE;
      }
    case SQL_PL_DATE:
      return SQL_C_TYPE_DATE;
    case SQL_PL_TIME:
      return SQL_C_TYPE_TIME;
    case SQL_PL_TIMESTAMP:
      return SQL_C_TIMESTAMP;
    default:
      assert(0);
      return CVNERR;
  }
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
pl_put_column(context *c, int nth, term_t col)

    Put the nth (0-based) result column of the statement in the Prolog
    variable col.  If the source(true) option is in effect, bind col to
    column(Table, Column, Value)

    If ptr_value is NULL, prepare_result() has not used SQLBindCol() due
    to a potentionally too large field such as for SQL_LONGVARCHAR
    columns.

There is a lot of odd code around the SQL_SERVER_BUG below. It turns out
Microsoft SQL Server sometimes gives the wrong length indication as well
as the wrong number of pad bytes for the first part of the data.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static void
put_chars(term_t val, int plTypeID, size_t len, const char *chars)
{ switch( plTypeID )
  { case SQL_PL_DEFAULT:
    case SQL_PL_ATOM:
      PL_put_atom_nchars(val, len, chars);
      break;
    case SQL_PL_STRING:
      PL_put_string_nchars(val, len, chars);
      break;
    case SQL_PL_CODES:
      PL_put_list_ncodes(val, len, chars);
      break;
    default:
      assert(0);
  }
}


static int
pl_put_column(context *c, int nth, term_t col)
{ parameter *p = &c->result[nth];
  term_t cell;
  term_t val;

  if ( true(c, CTX_SOURCE) )
  { cell = PL_new_term_refs(3);

    PL_put_atom(cell+0, p->source.table);
    PL_put_atom(cell+1, p->source.column);
    val = cell+2;
  } else
  { val = col;
    cell = 0;				/* make compiler happy */
  }

  if ( !p->ptr_value )			/* use SQLGetData() */
  { char buf[256];
    char *data = buf;
    SQLLEN len;

    DEBUG(2, Sdprintf("Fetching value for column %d using SQLGetData()\n",
		      nth+1));

    c->rc = SQLGetData(c->hstmt, (UWORD)(nth+1), p->cTypeID,
		       buf, sizeof(buf), &len);

    if ( c->rc == SQL_SUCCESS || c->rc == SQL_SUCCESS_WITH_INFO )
    { DEBUG(2, Sdprintf("Got %ld bytes\n", len));
      if ( len == SQL_NULL_DATA )
      { put_sql_null(val, c->null);
	goto ok;
      } else if ( len == SQL_NO_TOTAL )
      { /* Don't know what to do here */
	return PL_warning("No support for SQL_NO_TOTAL.\n"
			  "Please report to bugs@swi-prolog.org");
      } else if ( len >= (SDWORD)sizeof(buf) )
      { int pad = p->cTypeID == SQL_C_CHAR ? 1 : 0;
	size_t todo = len-sizeof(buf)+2*pad;
	SQLLEN len2;
	char *ep;
	int part = 2;

	data = odbc_malloc(len+pad);
	memcpy(data, buf, sizeof(buf));	/* you don't get the data twice! */
	ep = data+sizeof(buf)-pad;
#ifdef SQL_SERVER_BUG			/* compensate for wrong pad info */
        while(ep>data && ep[-1] == 0)
	{ ep--;
	  todo++;
	}
#endif
	while(todo > 0)
	{ c->rc = SQLGetData(c->hstmt, (UWORD)(nth+1), p->cTypeID,
			     ep, todo, &len2);
	  DEBUG(2, Sdprintf("Requested %d bytes for part %d; \
			     pad=%d; got %ld\n",
			    todo, part, pad, len2));
	  todo -= len2;
	  ep += len2;
	  part++;

	  switch( c->rc )
	  { case SQL_SUCCESS:
	      len = ep-data;
	      goto got_all_data;
	    case SQL_SUCCESS_WITH_INFO:
	      break;
	    default:
	    { Sdprintf("ERROR: %d\n", c->rc);
	      free(data);
	      return report_status(c);
	    }
	  }
	}
      }
    } else
    { DEBUG(1, Sdprintf("SQLGetData() returned %d\n", c->rc));
      return report_status(c);
    }

  got_all_data:
    put_chars(val, p->plTypeID, len, data);
    if ( data != buf )
      free(data);
    goto ok;
  }

  if ( p->length_ind == SQL_NULL_DATA )
  { put_sql_null(val, c->null);
  } else
  { switch( p->cTypeID )
    { case SQL_C_CHAR:
      case SQL_C_BINARY:
	put_chars(val, p->plTypeID, p->length_ind, (char*)p->ptr_value);
	break;
      case SQL_C_SLONG:
	PL_put_integer(val,*(SQLINTEGER *)p->ptr_value);
	break;
      case SQL_C_SBIGINT:
	PL_put_int64(val, *(SQLBIGINT *)p->ptr_value);
	break;
      case SQL_C_DOUBLE:
	PL_put_float(val,*(SQLDOUBLE *)p->ptr_value);
	break;
      case SQL_C_TYPE_DATE:
      { DATE_STRUCT* ds = (DATE_STRUCT*)p->ptr_value;
	term_t av = PL_new_term_refs(3);

	PL_put_integer(av+0, ds->year);
	PL_put_integer(av+1, ds->month);
	PL_put_integer(av+2, ds->day);

	PL_cons_functor_v(val, FUNCTOR_date3, av);
	break;
      }
      case SQL_C_TYPE_TIME:
      { TIME_STRUCT* ts = (TIME_STRUCT*)p->ptr_value;
	term_t av = PL_new_term_refs(3);

	PL_put_integer(av+0, ts->hour);
	PL_put_integer(av+1, ts->minute);
	PL_put_integer(av+2, ts->second);

	PL_cons_functor_v(val, FUNCTOR_time3, av);
	break;
      }
      case SQL_C_TIMESTAMP:
      { SQL_TIMESTAMP_STRUCT* ts = (SQL_TIMESTAMP_STRUCT*)p->ptr_value;

	switch( p->plTypeID )
	{ case SQL_PL_DEFAULT:
	  case SQL_PL_TIMESTAMP:
	  { term_t av = PL_new_term_refs(7);

	    PL_put_integer(av+0, ts->year);
	    PL_put_integer(av+1, ts->month);
	    PL_put_integer(av+2, ts->day);
	    PL_put_integer(av+3, ts->hour);
	    PL_put_integer(av+4, ts->minute);
	    PL_put_integer(av+5, ts->second);
	    PL_put_integer(av+6, ts->fraction);

	    PL_cons_functor_v(val, FUNCTOR_timestamp7, av);
	    break;
	  }
	  case SQL_PL_INTEGER:
	  case SQL_PL_FLOAT:
#ifdef HAVE_TIMEGM
	  { struct tm tm;
	    time_t t;

#ifndef USE_UTC
	    t = time(NULL);
	    tm = *localtime(&t);
#else
	    memset(&tm, 0, sizeof(tm));
#endif
	    tm.tm_year  = ts->year - 1900;
	    tm.tm_mon   = ts->month-1;
	    tm.tm_mday  = ts->day;
	    tm.tm_hour  = ts->hour;
	    tm.tm_min   = ts->minute;
	    tm.tm_sec   = ts->second;

#ifdef USE_UTC
	    t = timegm(&tm);
#else
	    t = mktime(&tm);
#endif

	    if ( p->plTypeID == SQL_PL_INTEGER )
	      PL_put_integer(val, t);
	    else
	      PL_put_float(val, (double)t); /* TBD: fraction */
	  }
#else
	    return PL_warning("System doesn't support mktime()/timegm()");
#endif
	    break;
	  default:
	    assert(0);
	}
	break;
      }
      default:
	return PL_warning("ODBC: Unknown cTypeID: %d",
			  p->cTypeID);
    }
  }

ok:
  if ( true(c, CTX_SOURCE) )
    PL_cons_functor_v(col, FUNCTOR_column3, cell);

  return TRUE;
}




/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Store a row
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
pl_put_row(term_t row, context *c)
{ term_t columns = PL_new_term_refs(c->NumCols);
  SQLSMALLINT i;

  for (i=0; i<c->NumCols; i++)
  { if ( !pl_put_column(c, i, columns+i) )
      return FALSE;			/* with exception */
  }

  PL_cons_functor_v(row, c->db_row, columns);

  return TRUE;
}


#ifdef EMULATE_TIMEGM

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
timegm() is provided by glibc and the inverse of gmtime().  The glibc
library suggests using mktime with TZ=UTC as alternative.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static time_t
timegm(struct tm *tm)
{ char *otz = getenv("TZ");
  time_t t;
  char oenv[20];

  if ( otz && strlen(otz) < 10 )	/* avoid buffer overflow */
  { putenv("TZ=UTC");
    t = mktime(tm);
    strcpy(oenv, "TZ=");
    strcat(oenv, otz);
    putenv(oenv);
  } else if ( otz )
  { Sdprintf("Too long value for TZ: %s", otz);
    t = mktime(tm);
  } else				/* not set, what to do? */
  { t = mktime(tm);
  }

  return t;
}


#endif
