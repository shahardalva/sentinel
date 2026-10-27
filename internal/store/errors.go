package store

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
)

// Typed errors, for errors.Is.
var (
	// ErrNotFound: no tenant or key with that ID. A malformed ID is not
	// found too.
	ErrNotFound = errors.New("store: not found")
	// ErrDuplicateName: a tenant with that name already exists.
	ErrDuplicateName = errors.New("store: duplicate tenant name")
	// ErrTenantDisabled: the tenant is disabled, so it gets no new keys.
	ErrTenantDisabled = errors.New("store: tenant disabled")
	// ErrInvalidName: a tenant name that is empty, too long, or holds a
	// control character or surrounding whitespace.
	ErrInvalidName = errors.New("store: invalid tenant name")
	// ErrSchemaTooNew: the database has a schema version newer than this
	// binary knows. An older gateway must not run against it.
	ErrSchemaTooNew = errors.New("store: database schema is newer than this binary")
)

// Error is a sanitized database error. Its message holds only the
// operation, a fixed description, and for a server error its SQLSTATE code
// and constraint name, never pgx's own message: pgx's connect errors name
// the user, database and host:port, and the server's messages can name the
// user or database too ("password authentication failed for user ...").
//
// The original error is dropped, not wrapped, so nothing that walks the
// chain can print it. Unwrap returns only context.Canceled or
// context.DeadlineExceeded, when one caused it.
type Error struct {
	Op string
	// Code is the SQLSTATE code of a server error, such as "23505" or
	// "28P01"; empty for any other error.
	Code string
	// Constraint is the violated constraint's name, for a server error
	// that has one. Constraint names come from this package's migrations.
	Constraint string
	desc       string
	ctxErr     error
}

func (e *Error) Error() string {
	msg := "store: " + e.Op + ": " + e.desc
	if e.Code != "" {
		msg += " (SQLSTATE " + e.Code
		if e.Constraint != "" {
			msg += ", constraint " + e.Constraint
		}
		msg += ")"
	}
	return msg
}

func (e *Error) Unwrap() error { return e.ctxErr }

// sanitize turns any error from pgx into an *Error safe to log. A nil err
// stays nil; an error this package made already passes through.
func sanitize(op string, err error) error {
	if err == nil {
		return nil
	}
	var se *Error
	if errors.As(err, &se) {
		return err
	}
	for _, typed := range []error{ErrNotFound, ErrDuplicateName, ErrTenantDisabled, ErrInvalidName, ErrSchemaTooNew} {
		if errors.Is(err, typed) {
			return err
		}
	}
	e := &Error{Op: op, desc: "database error"}
	switch {
	case errors.Is(err, context.Canceled):
		e.ctxErr, e.desc = context.Canceled, "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		e.ctxErr, e.desc = context.DeadlineExceeded, "timed out"
	}
	var connErr *pgconn.ConnectError
	isConnect := errors.As(err, &connErr)
	if isConnect && e.ctxErr == nil {
		e.desc = "cannot connect to postgres"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		e.Code, e.Constraint = pgErr.Code, pgErr.ConstraintName
		if !isConnect {
			e.desc = "postgres error"
		}
		return e
	}
	var netErr net.Error
	if e.ctxErr == nil && !isConnect && errors.As(err, &netErr) {
		if netErr.Timeout() {
			e.desc = "network timeout"
		} else {
			e.desc = "network error"
		}
	}
	if e.desc == "database error" {
		// The type name is safe and says which kind of failure it was.
		e.desc = fmt.Sprintf("database error (%T)", err)
	}
	return e
}
