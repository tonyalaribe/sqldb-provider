package main

import (
	"log"

	"gitlab.com/middlefront/sqldb-provider/cmd"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
func main() {
	cmd.Execute()
}
