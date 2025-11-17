package utils

import "log"

func Log(prefix, format string, values ...any) {
	log.Printf(prefix+" "+format+"\n", values...)
}
