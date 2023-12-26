// go run Commons.go PromBasic.go
package main

import (
	"fmt"
)

func main() {
	sqlC := newSqlConnect()
	defer func() { _ = sqlC.Close() }()

	fmt.Printf("Driver: %s\n", sqlC.GetDriver())
	fmt.Printf("DSN   : %s\n", sqlC.GetDsn())
	fmt.Printf("Flavor: %s\n", sqlC.GetDbFlavor())
}
