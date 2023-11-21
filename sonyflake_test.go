package sonyflake

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"
)

var sf *Sonyflake

var startTime int64
var machineID uint64

func init() {
	var (
		st  Settings
		err error
	)
	st.StartTime = time.Now()

	sf, err = NewSonyflake(st)
	if err != nil {
		panic(err)
	}
	if sf == nil {
		panic("sonyflake not created")
	}

	startTime = toSonyflakeTime(st.StartTime)

	ip, idErr := MachineID()
	if idErr != nil {
		panic(idErr)
	}
	machineID = uint64(ip)
}

func nextID(t *testing.T) uint64 {
	id, err := sf.NextID()
	if err != nil {
		t.Fatal("id not generated")
	}
	return id
}

func TestSonyflakeOnce(t *testing.T) {
	sleepTime := uint64(50)
	time.Sleep(time.Duration(sleepTime) * 10 * time.Millisecond)

	id := nextID(t)
	parts := Decompose(id)

	actualMSB := parts["msb"]
	if actualMSB != 0 {
		t.Errorf("unexpected msb: %d", actualMSB)
	}

	actualTime := parts["time"]
	if actualTime < sleepTime || actualTime > sleepTime+1 {
		t.Errorf("unexpected time: %d", actualTime)
	}

	actualSequence := parts["sequence"]
	if actualSequence != 0 {
		t.Errorf("unexpected sequence: %d", actualSequence)
	}

	actualMachineID := parts["machine-id"]
	if actualMachineID != machineID {
		t.Errorf("unexpected machine id: %d", actualMachineID)
	}

	fmt.Println("sonyflake id:", id)
	fmt.Println("decompose:", parts)
}

func currentTime() int64 {
	return toSonyflakeTime(time.Now())
}

func TestSonyflakeFor10Sec(t *testing.T) {
	var numID uint32
	var lastID uint64
	var maxSequence uint64

	initial := currentTime()
	current := initial
	for current-initial < 1000 {
		id := nextID(t)
		parts := Decompose(id)
		numID++

		if id <= lastID {
			t.Fatal("duplicated id")
		}
		lastID = id

		current = currentTime()

		actualMSB := parts["msb"]
		if actualMSB != 0 {
			t.Errorf("unexpected msb: %d", actualMSB)
		}

		actualTime := int64(parts["time"])
		overtime := startTime + actualTime - current
		if overtime > 0 {
			t.Errorf("unexpected overtime: %d", overtime)
		}

		actualSequence := parts["sequence"]
		if maxSequence < actualSequence {
			maxSequence = actualSequence
		}

		actualMachineID := parts["machine-id"]
		if actualMachineID != machineID {
			t.Errorf("unexpected machine id: %d", actualMachineID)
		}
	}

	if maxSequence != 1<<BitLenSequence-1 {
		t.Errorf("unexpected max sequence: %d", maxSequence)
	}
	fmt.Println("max sequence:", maxSequence)
	fmt.Println("number of id:", numID)
}

func TestSonyflakeInParallel(t *testing.T) {
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)
	fmt.Println("number of cpu:", numCPU)

	consumer := make(chan uint64)

	const numID = 10000
	generate := func() {
		for i := 0; i < numID; i++ {
			consumer <- nextID(t)
		}
	}

	const numGenerator = 10
	for i := 0; i < numGenerator; i++ {
		go generate()
	}

	set := make(map[uint64]struct{})
	for i := 0; i < numID*numGenerator; i++ {
		id := <-consumer
		if _, ok := set[id]; ok {
			t.Fatal("duplicated id")
		}
		set[id] = struct{}{}
	}
	fmt.Println("number of id:", len(set))
}

func TestNilSonyflake(t *testing.T) {
	var (
		startInFuture Settings
		flake         *Sonyflake
		err           error
	)
	startInFuture.StartTime = time.Now().Add(time.Duration(1) * time.Minute)
	flake, err = NewSonyflake(startInFuture)
	if err != ErrInvalidStartTime || flake != nil {
		t.Error("Unexpected error", err)
	}

	var noMachineID Settings
	noMachineID.MachineID = func() (uint16, error) {
		return 0, fmt.Errorf("no machine id")
	}
	flake, err = NewSonyflake(noMachineID)
	if err == nil || err.Error() != "no machine id" {
		t.Error("unexpected error", err)
	}

	var invalidMachineID Settings
	invalidMachineID.CheckMachineID = func(uint16) bool {
		return false
	}
	flake, err = NewSonyflake(invalidMachineID)
	if err != ErrCheckMachineIDFailed {
		t.Error("unexpected error", err)
	}
}

func pseudoSleep(period time.Duration) {
	sf.startTime -= int64(period) / sonyflakeTimeUnit
}

func TestNextIDError(t *testing.T) {
	year := time.Duration(365*24) * time.Hour
	pseudoSleep(time.Duration(174) * year)
	nextID(t)

	pseudoSleep(time.Duration(1) * year)
	_, err := sf.NextID()
	if err == nil {
		t.Errorf("time is not over")
	}
}

func TestReproducibleIDs(t *testing.T) {
	now := time.Date(2021, 7, 29, 1, 2, 4, 8, time.UTC)
	flake, err := NewSonyflake(Settings{
		StartTime: now.Add(-time.Second),
		MachineID: func() (uint16, error) {
			return 127, nil
		},
	})

	if err != nil {
		t.Error("Failed to create sonyflake", err)
	}

	// Ensure we exhaust the sequence range for the current timestamp
	const numIDs = 1<<BitLenSequence + 1

	// sample the 2 first and the 2 last IDs in the range
	var id0, id1, idNMinusOne, idN map[string]uint64
	for i := 0; i < numIDs; i++ {
		id, err := flake.NextReproducibleID(now)
		if err != nil {
			panic(err)
		}
		switch i {
		case 0:
			id0 = Decompose(id)
		case 1:
			id1 = Decompose(id)
		case numIDs - 2:
			idNMinusOne = Decompose(id)
		case numIDs - 1:
			idN = Decompose(id)
		}
	}

	if !reflect.DeepEqual(id0, map[string]uint64{
		"id":         1677721727,
		"msb":        0,
		"time":       100,
		"sequence":   0,
		"machine-id": 127,
	}) {
		t.Errorf("id0 = %v", id0)
	}

	if !reflect.DeepEqual(id1, map[string]uint64{
		"id":         1677787263,
		"msb":        0,
		"time":       100,
		"sequence":   1,
		"machine-id": 127,
	}) {
		t.Errorf("id1 = %v", id1)
	}

	if !reflect.DeepEqual(idNMinusOne, map[string]uint64{
		"id":         1694433407,
		"msb":        0,
		"time":       100,
		"sequence":   255,
		"machine-id": 127,
	}) {
		t.Errorf("idNMinusOne = %v", idNMinusOne)
	}

	if !reflect.DeepEqual(idN, map[string]uint64{
		"id":         1694498943,
		"msb":        0,
		"time":       101,
		"sequence":   0, // note that sequence have been exhausted for "now" and "time" advanced
		"machine-id": 127,
	}) {
		t.Errorf("idN = %v", idN)
	}
}

func TestBufferInc(t *testing.T) {
	now := time.Date(2021, 7, 29, 1, 2, 4, 8, time.UTC)
	flake, err := NewSonyflake(Settings{
		StartTime: now.Add(-time.Second),
		MachineID: func() (uint16, error) {
			return 127, nil
		},
	})

	if err != nil {
		t.Error("Failed to create sonyflake", err)
	}

	startID, err := flake.NextID()
	if err != nil {
		t.Error("failed to create initial ID", err)
	}

	var buf Buffer
	DecomposeToBuffer(startID, &buf)

	const numTests = 100_000
	seenIDs := make(map[uint64]struct{}, numTests)
	for i := 0; i < numTests; i++ {
		id, err := buf.Compose()
		if err != nil {
			t.Error("failed to create id from buffer", err)
		}
		if _, ok := seenIDs[id]; ok {
			t.Error("id", id, "already seen, step:", i)
		}
		seenIDs[id] = struct{}{}
		buf.Inc()
	}
}
