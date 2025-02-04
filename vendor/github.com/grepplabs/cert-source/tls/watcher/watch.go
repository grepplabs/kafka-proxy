package watcher

import (
	"fmt"
	"log/slog"
	"reflect"
	"time"
)

func Watch[T any, PT interface {
	GetChecksum() []byte
	*T
}](logger *slog.Logger, ch chan T, refresh time.Duration, init PT, loadFn func() (PT, error), changedFn func()) {
	once := refresh <= 0

	if refresh < time.Second {
		refresh = time.Second
	}
	logger.Info(fmt.Sprintf("cert watch is started, refresh interval %s", refresh))

	var last = init
	for {
		next, err := loadFn()
		if err != nil {
			logger.Error("cannot load certificates", slog.String("error", err.Error()))
			time.Sleep(refresh)
			continue
		}
		if last != nil {
			if reflect.DeepEqual(next.GetChecksum(), last.GetChecksum()) {
				if once && init != nil {
					// init value is set, so assume it was already sent to channel
					logger.Info("cert watch is disabled")
					return
				}
				time.Sleep(refresh)
				continue
			}
		}

		ch <- *next
		last = next

		if changedFn != nil {
			changedFn()
		}
		if once {
			logger.Info("cert watch is disabled")
			return
		}
		time.Sleep(refresh)
	}
}
