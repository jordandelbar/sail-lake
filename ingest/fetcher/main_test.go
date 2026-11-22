package main

import (
	"encoding/json"
	"testing"
	"time"
)

const sample = `{"features":[
 {"id":"us1","properties":{"time":1,"updated":2,"mag":4.5,"magType":"mb","place":"10 km N of X, Peru","type":"earthquake","tsunami":0},
  "geometry":{"coordinates":[-75.1,-12.2,35.0]}},
 {"id":"ak2","properties":{"time":3,"updated":4,"mag":null,"magType":null,"place":null,"type":"earthquake","tsunami":1},
  "geometry":{"coordinates":[-150.0,61.0,10.0]}},
 {"id":"bad","properties":{},"geometry":{"coordinates":[1.0]}},
 {"id":"","properties":{"time":1,"updated":2},"geometry":{"coordinates":[1.0,2.0,3.0]}},
 {"id":"notime","properties":{"updated":2},"geometry":{"coordinates":[1.0,2.0,3.0]}},
 {"id":"nulllat","properties":{"time":1,"updated":2},"geometry":{"coordinates":[1.0,null,3.0]}}
]}`

func TestParseFeed(t *testing.T) {
	events, err := parseFeed([]byte(sample), "feed")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 events (bad, no id, no time, null coordinate skipped), got %d", len(events))
	}
	e := events[0]
	if e.ID != "us1" || *e.Mag != 4.5 || e.Lon != -75.1 || e.Lat != -12.2 || e.Depth != 35.0 || e.MsgID() != "feed:us1:2" {
		t.Fatalf("bad mapping: %+v", e)
	}
	// nulls stay null in the output, Sail reads them as missing values
	b, _ := json.Marshal(events[1])
	want := `{"id":"ak2","time":3,"updated":4,"mag":null,"magType":null,"place":null,"type":"earthquake","tsunami":1,"lon":-150,"lat":61,"depth":10,"source":"feed","observed":0}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
	if _, err := parseFeed([]byte("not json"), "feed"); err == nil {
		t.Fatal("want error on invalid json")
	}
}

func TestIntEnv(t *testing.T) {
	for _, tc := range []struct {
		val  string
		min  int
		want int
		ok   bool
	}{{"60", 1, 60, true}, {"0", 1, 0, false}, {"abc", 0, 0, false}, {"-3", 0, 0, false}, {"0", 0, 0, true}} {
		t.Setenv("X_TEST", tc.val)
		got, err := intEnv("X_TEST", "1", tc.min)
		if (err == nil) != tc.ok || got != tc.want {
			t.Errorf("intEnv(%q, min %d) = %d, %v", tc.val, tc.min, got, err)
		}
	}
}

func TestWindows(t *testing.T) {
	day := 24 * time.Hour
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w := windows(from, from.Add(10*day), 7*day)
	if len(w) != 2 || w[0][1] != from.Add(7*day) || w[1][0] != from.Add(7*day) || w[1][1] != from.Add(10*day) {
		t.Fatalf("want [0,7) [7,10), got %v", w)
	}
	if len(windows(from, from, 7*day)) != 0 {
		t.Fatal("empty range must give no window")
	}
}
