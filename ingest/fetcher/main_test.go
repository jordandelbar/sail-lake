package main

import (
	"encoding/json"
	"testing"
)

const sample = `{"features":[
 {"id":"us1","properties":{"time":1,"updated":2,"mag":4.5,"magType":"mb","place":"10 km N of X, Peru","type":"earthquake","tsunami":0},
  "geometry":{"coordinates":[-75.1,-12.2,35.0]}},
 {"id":"ak2","properties":{"time":3,"updated":4,"mag":null,"magType":null,"place":null,"type":"earthquake","tsunami":1},
  "geometry":{"coordinates":[-150.0,61.0,10.0]}},
 {"id":"bad","properties":{},"geometry":{"coordinates":[1.0]}}
]}`

func TestParseFeed(t *testing.T) {
	events, err := parseFeed([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 events (short coordinates skipped), got %d", len(events))
	}
	e := events[0]
	if e.ID != "us1" || *e.Mag != 4.5 || e.Lon != -75.1 || e.Lat != -12.2 || e.Depth != 35.0 || e.MsgID() != "us1:2" {
		t.Fatalf("bad mapping: %+v", e)
	}
	// nulls stay null in the output, Sail reads them as missing values
	b, _ := json.Marshal(events[1])
	want := `{"id":"ak2","time":3,"updated":4,"mag":null,"magType":null,"place":null,"type":"earthquake","tsunami":1,"lon":-150,"lat":61,"depth":10}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
	if _, err := parseFeed([]byte("not json")); err == nil {
		t.Fatal("want error on invalid json")
	}
}
