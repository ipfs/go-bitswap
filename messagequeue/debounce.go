package messagequeue

import "time"

// https://gist.github.com/leolara/d62b87797b0ef5e418cd#gistcomment-2243168
func Debounce(interval time.Duration, f func()) func() {
	var timer *time.Timer

	return func() {
		// TODO: Is this threadsafe?
		if timer == nil {
			timer = time.NewTimer(interval)

			go func() {
				<-timer.C
				timer.Stop()
				timer = nil
				f()
			}()
		} else {
			timer.Reset(interval)
		}
	}
}
