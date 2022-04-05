package grpc

import (
	"go.unistack.org/micro/v3/errors"
	"google.golang.org/grpc/status"
)

func microError(err error) error {
	// no error

	if err == nil {
		// nothing to do
		return nil
	}

	if verr, ok := err.(*errors.Error); ok {
		// micro error
		return verr
	}

	// grpc error
	s, ok := status.FromError(err)
	if !ok {
		// can't get status detals from grpc error, return base error
		return err
	}

	details := s.Details()
	switch len(details) {
	case 0:
		if verr := errors.Parse(s.Message()); verr.Code > 0 {
			// return micro error
			return verr
		}
		// return base error as it not micro error
		return err
	case 1:
		if verr, ok := details[0].(*errors.Error); ok {
			// return nested micro error
			return verr
		}
		// return base error as it not holds micro error
		return err
	}

	// attached messages in details more then 1, try to fallback to micro error
	if verr := errors.Parse(s.Message()); verr.Code > 0 {
		// return micro error
		return verr
	}

	// not micro error return base error
	return err
}
