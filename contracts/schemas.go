package contracts

import "embed"

// Schemas holds the JSON Schemas of the events (schemas/*.json), so that Go code can validate
// events against the committed contracts without reading the repository at run time.
//
//go:embed schemas/*.json
var Schemas embed.FS
