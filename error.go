package grpc

import (
	"go.unistack.org/micro/v3/errors"
	"google.golang.org/grpc/status"
)

func microError(err error) error {
	// no error

	if err == nil {
		return nil
	}

	if verr, ok := err.(*errors.Error); ok {
		return verr
	}

	// grpc error
	s, ok := status.FromError(err)
	if !ok {
		return err
	}

	details := s.Details()
	switch len(details) {
	case 0:
		return err
	case 1:
		if verr, ok := details[0].(error); ok {
			return microError(verr)
		}
		return err
	default:
		if e := errors.Parse(s.Message()); e.Code > 0 {
			return e // actually a micro error
		}
		return err
	}
}
