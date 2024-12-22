package utils

import (
	"errors"
	"log"
	"time"
)

func Retry(attempts int, delay time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		log.Printf("Erro na tentativa %d: %v. Retentando em %s...", i+1, err, delay)
		time.Sleep(delay)
		delay *= 2
	}
	return errors.New("número máximo de tentativas alcançado: " + err.Error())
}
