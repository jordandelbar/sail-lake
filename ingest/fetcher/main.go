// Fetcher polls the USGS earthquake feed and publishes each event to JetStream.
// With BACKFILL_DAYS set, it instead publishes that many days of history from the USGS
// FDSN event API, week by week, and exits.
//
// Re-fetches overlap; JetStream drops repeats via Nats-Msg-Id (id:updated) within the
// stream's duplicate window.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Event is what lands on quakes.raw (and later in s3://lake/raw/quakes/).
type Event struct {
	ID      string   `json:"id"`
	Time    int64    `json:"time"`
	Updated int64    `json:"updated"`
	Mag     *float64 `json:"mag"` // null for some events
	MagType *string  `json:"magType"`
	Place   *string  `json:"place"`
	Type    string   `json:"type"`
	Tsunami int      `json:"tsunami"`
	Lon     float64  `json:"lon"`
	Lat     float64  `json:"lat"`
	Depth   float64  `json:"depth"`
	Source  string   `json:"source"` // "feed" (live, revisions over time) or "backfill" (final version only)
}

// MsgID is the JetStream dedup key: a revised event (new `updated`) gets through.
func (e Event) MsgID() string { return fmt.Sprintf("%s:%d", e.ID, e.Updated) }

type feed struct {
	Features []struct {
		ID         string `json:"id"`
		Properties struct {
			Time    int64    `json:"time"`
			Updated int64    `json:"updated"`
			Mag     *float64 `json:"mag"`
			MagType *string  `json:"magType"`
			Place   *string  `json:"place"`
			Type    string   `json:"type"`
			Tsunami int      `json:"tsunami"`
		} `json:"properties"`
		Geometry struct {
			Coordinates []float64 `json:"coordinates"` // lon, lat, depth
		} `json:"geometry"`
	} `json:"features"`
}

// parseFeed maps a GeoJSON feed to events, skipping features without full coordinates.
func parseFeed(body []byte, source string) ([]Event, error) {
	var f feed
	if err := json.Unmarshal(body, &f); err != nil {
		return nil, fmt.Errorf("decode feed: %w", err)
	}
	events := make([]Event, 0, len(f.Features))
	for _, ft := range f.Features {
		c, p := ft.Geometry.Coordinates, ft.Properties
		if len(c) < 3 {
			log.Printf("skip %s: %d coordinates", ft.ID, len(c))
			continue
		}
		events = append(events, Event{
			ID: ft.ID, Time: p.Time, Updated: p.Updated, Mag: p.Mag, MagType: p.MagType,
			Place: p.Place, Type: p.Type, Tsunami: p.Tsunami, Lon: c[0], Lat: c[1], Depth: c[2],
			Source: source,
		})
	}
	return events, nil
}

func fetch(ctx context.Context, url, source string) ([]Event, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "sail-lake-demo")
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req) // FDSN week queries take a few seconds
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("feed: %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20)) // all_day ~300KB, an FDSN week a few MB
	if err != nil {
		return nil, err
	}
	return parseFeed(body, source)
}

const fdsn = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&orderby=time-asc"

// windows splits [from, to) into consecutive spans of at most step. The FDSN API refuses
// queries matching more than 20000 events (it errors, it does not truncate); a week is ~2-3k.
func windows(from, to time.Time, step time.Duration) [][2]time.Time {
	var out [][2]time.Time
	for start := from; start.Before(to); start = start.Add(step) {
		end := start.Add(step)
		if end.After(to) {
			end = to
		}
		out = append(out, [2]time.Time{start, end})
	}
	return out
}

func backfill(ctx context.Context, js jetstream.JetStream, days int) error {
	to := time.Now().UTC()
	total := 0
	for _, w := range windows(to.AddDate(0, 0, -days), to, 7*24*time.Hour) {
		const layout = "2006-01-02T15:04:05"
		url := fmt.Sprintf("%s&starttime=%s&endtime=%s", fdsn, w[0].Format(layout), w[1].Format(layout))
		events, err := fetch(ctx, url, "backfill")
		if err != nil {
			return fmt.Errorf("window %s: %w", w[0].Format(time.DateOnly), err)
		}
		fresh, err := publish(ctx, js, events)
		if err != nil {
			return err
		}
		total += fresh
		log.Printf("backfill %s..%s fetched=%d new=%d", w[0].Format(time.DateOnly), w[1].Format(time.DateOnly), len(events), fresh)
	}
	log.Printf("backfill done: %d events published", total)
	return nil
}

func publish(ctx context.Context, js jetstream.JetStream, events []Event) (int, error) {
	fresh := 0
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			return fresh, err
		}
		ack, err := js.Publish(ctx, "quakes.raw", data, jetstream.WithMsgID(e.MsgID()))
		if err != nil {
			return fresh, err
		}
		if !ack.Duplicate {
			fresh++
		}
	}
	return fresh, nil
}

// ensureStream creates the QUAKES stream, or leaves it as is when it already exists.
// all_day items live 24h in the feed, the 25h dedup window covers the overlap.
func ensureStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "QUAKES", Subjects: []string{"quakes.>"},
		Duplicates: 25 * time.Hour, MaxAge: 7 * 24 * time.Hour,
	})
	return err
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func main() {
	url := env("FEED", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson")
	every, err := strconv.Atoi(env("EVERY_S", "60"))
	if err != nil {
		log.Fatalf("EVERY_S: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(env("NATS_URL", "nats://nats:4222"))
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	defer nc.Drain()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}
	if days, _ := strconv.Atoi(env("BACKFILL_DAYS", "0")); days > 0 {
		if err := ensureStream(ctx, js); err != nil {
			log.Fatalf("stream: %v", err)
		}
		if err := backfill(ctx, js, days); err != nil {
			log.Fatalf("backfill: %v", err)
		}
		return
	}

	tick := time.NewTicker(time.Duration(every) * time.Second)
	defer tick.Stop()
	for {
		// keep polling through feed/network/NATS blips, errors are logged per round.
		// The stream is (re)created every round, so a NATS that lost its state heals itself.
		err := ensureStream(ctx, js)
		var events []Event
		if err == nil {
			events, err = fetch(ctx, url, "feed")
		}
		if err == nil {
			var fresh int
			fresh, err = publish(ctx, js, events)
			log.Printf("fetched=%d new=%d", len(events), fresh)
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("error: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
