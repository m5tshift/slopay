package ledger

import "time"

// SetNow replaces the clock of the service.
func SetNow(s *Service, now func() time.Time) { s.now = now }
