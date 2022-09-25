package prom

import (
	"testing"
)

// TestFailedWithMsgFunc is called when test run fails.
//
// prom's internal use only!
type TestFailedWithMsgFunc func(msg string)

// TestSetupOrTeardownFunc is called before and after a test run.
//
// prom's internal use only!
type TestSetupOrTeardownFunc func(t *testing.T, testName string)

func setupTest(t *testing.T, testName string, extraSetupFunc, extraTeardownFunc TestSetupOrTeardownFunc) func(t *testing.T) {
	if extraSetupFunc != nil {
		extraSetupFunc(t, testName)
	}
	return func(t *testing.T) {
		if extraTeardownFunc != nil {
			extraTeardownFunc(t, testName)
		}
	}
}
