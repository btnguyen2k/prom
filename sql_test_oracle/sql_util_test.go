package sql

import (
	"fmt"
	promsql "github.com/btnguyen2k/prom/sql"
	"testing"
	"time"
)

func TestUtil_DurationToOracleDayToSecond(t *testing.T) {
	testName := "TestUtil_DurationToOracleDayToSecond"
	testCases := []struct {
		duration  time.Duration
		precision int
		expected  string
	}{
		{99 * time.Second, -1, "0 00:01:39"},
		{123 * time.Second, 0, "0 00:02:03"},
		{234567 * time.Millisecond, 1, "0 00:03:54.6"},
		{345678901 * time.Millisecond, 2, "4 00:01:18.90"},
		{45678901234 * time.Microsecond, 3, "0 12:41:18.901"},
		{5678901234567 * time.Nanosecond, 4, "0 01:34:38.9012"},
		{56789012345678 * time.Nanosecond, 5, "0 15:46:29.01235"},
		{567890123456789 * time.Nanosecond, 6, "6 13:44:50.123457"},
		{5678901234567890 * time.Nanosecond, 7, "65 17:28:21.2345679"},
		{6789012345678901 * time.Nanosecond, 8, "78 13:50:12.34567890"},
		{7890123456789012 * time.Nanosecond, 9, "91 07:42:03.456789012"},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-precision{%d}", tc.duration, tc.precision), func(t *testing.T) {
			val := promsql.DurationToOracleDayToSecond(tc.duration, tc.precision)
			if val != tc.expected {
				t.Fatalf("%s failed: expected [%s] but received [%s]", testName, tc.expected, val)
			}
		})
	}
}

func TestUtil_ParseOracleIntervalDayToSecond(t *testing.T) {
	testName := "TestUtil_ParseOracleIntervalDayToSecond"
	testCases := []struct {
		input    string
		expected time.Duration
	}{
		{"+0 00:01:39", (1*60 + 39) * time.Second},
		{"+0 00:02:03", (2*60 + 03) * time.Second},
		{"+0 00:03:54.6", (3*60+54)*time.Second + 600*time.Millisecond},
		{"+4 00:01:18.90", 4*24*time.Hour + (1*60+18)*time.Second + 900*time.Millisecond},
		{"+0 12:41:18.901", (12*60*60+41*60+18)*time.Second + 901*time.Millisecond},
		{"+0 01:34:38.9012", (1*60*60+34*60+38)*time.Second + 901200*time.Microsecond},
		{"+0 15:46:29.01235", (15*60*60+46*60+29)*time.Second + 12350*time.Microsecond},
		{"+6 13:44:50.123457", 6*24*time.Hour + (13*60*60+44*60+50)*time.Second + 123457*time.Microsecond},
		{"+65 17:28:21.2345679", 65*24*time.Hour + (17*60*60+28*60+21)*time.Second + 234567900*time.Nanosecond},
		{"+78 13:50:12.34567890", 78*24*time.Hour + (13*60*60+50*60+12)*time.Second + 345678900*time.Nanosecond},
		{"+91 07:42:03.456789012", 91*24*time.Hour + (7*60*60+42*60+03)*time.Second + 456789012*time.Nanosecond},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			val, err := promsql.ParseOracleIntervalDayToSecond(tc.input)
			if err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}
			if val != tc.expected {
				t.Fatalf("%s failed: expected [%s] but received [%s]", testName, tc.expected, val)
			}
		})
	}
}

func TestUtil_DurationToOracleYearToMonth(t *testing.T) {
	testName := "TestUtil_DurationToOracleYearToMonth"
	testCases := []struct {
		duration time.Duration
		expected string
	}{
		{123 * time.Hour, "0-0"},
		{2345 * time.Hour, "0-3"},
		{34567 * time.Hour, "4-0"},
		{456789 * time.Minute, "0-10"},
		{5678901 * time.Minute, "10-11"},
		{67890123 * time.Second, "2-2"},
		{789012345 * time.Second, "25-4"},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s", tc.duration), func(t *testing.T) {
			val := promsql.DurationToOracleYearToMonth(tc.duration)
			if val != tc.expected {
				t.Fatalf("%s failed: expected [%s] but received [%s]", testName, tc.expected, val)
			}
		})
	}
}

func TestUtil_ParseOracleIntervalYearToMonth(t *testing.T) {
	testName := "TestUtil_ParseOracleIntervalYearToMonth"
	testCases := []struct {
		input    string
		expected time.Duration
	}{
		{"+00-00", (0*12 + 0) * 30 * 24 * time.Hour},
		{"+0-03", (0*12 + 3) * 30 * 24 * time.Hour},
		{"+04-0", (4*12 + 0) * 30 * 24 * time.Hour},
		{"+00-10", (0*12 + 10) * 30 * 24 * time.Hour},
		{"+10-11", (10*12 + 11) * 30 * 24 * time.Hour},
		{"+02-2", (2*12 + 2) * 30 * 24 * time.Hour},
		{"+25-04", (25*12 + 4) * 30 * 24 * time.Hour},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			val, err := promsql.ParseOracleIntervalYearToMonth(tc.input)
			if err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}
			if val != tc.expected {
				t.Fatalf("%s failed: expected [%s] but received [%s]", testName, tc.expected, val)
			}
		})
	}
}
