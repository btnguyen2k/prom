package prom

import (
	"bytes"
	"net"
	"strconv"
	"strings"

	olaflib "github.com/btnguyen2k/consu/olaf"
)

func getMacAddr() string {
	interfaces, err := net.Interfaces()
	if err == nil {
		for _, i := range interfaces {
			if i.Flags&net.FlagUp != 0 && !bytes.Equal(i.HardwareAddr, nil) {
				// Don't use random as we have a real address
				return i.HardwareAddr.String()
			}
		}
	}
	return ""
}

func getMacAddrAsLong() int64 {
	mac, _ := strconv.ParseInt(strings.Replace(getMacAddr(), ":", "", -1), 16, 64)
	return mac
}

var olaf = olaflib.NewOlaf(getMacAddrAsLong())

// NewId generates a new unique id.
//
// prom's internal use only!
func NewId() string {
	return strings.ToLower(olaf.Id128Hex())
}
