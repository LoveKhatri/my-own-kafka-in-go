package main

import (
	"fmt"

	"github.com/google/uuid"
)

func main() {
	a := uuid.MustParse("99999999-9999-9999-9999-999999999999")

	fmt.Println(a[:])
}
