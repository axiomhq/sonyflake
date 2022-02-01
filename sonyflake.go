// Package sonyflake implements Sonyflake, a distributed unique ID generator inspired by Twitter's Snowflake.
//
// A Sonyflake ID is composed of
//     39 bits for time in units of 10 msec
//      8 bits for a sequence number
//     16 bits for a machine id
package sonyflake

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	ErrInvalidStartTime     = errors.New("StartTime cannot be after current time")
	ErrCheckMachineIDFailed = errors.New("CheckMachineID call returned false")
	ErrNoMachineID          = errors.New("failed to generate a valid machine id")
)

// These constants are the bit lengths of Sonyflake ID parts.
const (
	BitLenTime      = 39                               // bit length of time
	BitLenSequence  = 8                                // bit length of sequence number
	BitLenMachineID = 63 - BitLenTime - BitLenSequence // bit length of machine id
)

// Settings configures Sonyflake:
//
// StartTime is the time since which the Sonyflake time is defined as the elapsed time.
// If StartTime is 0, the start time of the Sonyflake is set to "2014-09-01 00:00:00 +0000 UTC".
// If StartTime is ahead of the current time, Sonyflake is not created.
//
// MachineID returns the unique ID of the Sonyflake instance.
// If MachineID returns an error, Sonyflake is not created.
// If MachineID is nil, default MachineID is used.
// Default MachineID returns the lower 16 bits of the private IP address.
//
// CheckMachineID validates the uniqueness of the machine ID.
// If CheckMachineID returns false, Sonyflake is not created.
// If CheckMachineID is nil, no validation is done.
type Settings struct {
	StartTime      time.Time
	MachineID      func() (uint16, error)
	CheckMachineID func(uint16) bool
}

// Sonyflake is a distributed unique ID generator.
type Sonyflake struct {
	mutex       *sync.Mutex
	startTime   int64
	elapsedTime int64
	sequence    uint16
	machineID   uint16
}

type Buffer struct {
	// Timestamp in units of 10ms
	Time      uint64
	Sequence  uint8
	MSB       uint8
	MachineID uint16
}

// NewSonyflake returns a new Sonyflake configured with the given Settings.
// NewSonyflake returns an error in the following cases:
// - Settings.StartTime is ahead of the current time. The error will be ErrInvalidStartTime.
// - Settings.MachineID returns an error. The error from the function will be returned.
// - Settings.CheckMachineID returns false, in which case the error is ErrCheckMachineIDFailed.
func NewSonyflake(st Settings) (*Sonyflake, error) {
	sf := new(Sonyflake)
	sf.mutex = new(sync.Mutex)
	sf.sequence = uint16(1<<BitLenSequence - 1)

	if st.StartTime.After(time.Now()) {
		return nil, ErrInvalidStartTime
	}
	if st.StartTime.IsZero() {
		sf.startTime = toSonyflakeTime(time.Date(2014, 9, 1, 0, 0, 0, 0, time.UTC))
	} else {
		sf.startTime = toSonyflakeTime(st.StartTime)
	}

	var err error
	if st.MachineID == nil {
		sf.machineID, err = MachineID()
	} else {
		sf.machineID, err = st.MachineID()
	}
	if err != nil {
		return nil, err
	}
	if st.CheckMachineID != nil && !st.CheckMachineID(sf.machineID) {
		return nil, ErrCheckMachineIDFailed
	}

	return sf, nil
}

// NextID generates a next unique ID.
// After the Sonyflake time overflows, NextID returns an error.
func (sf *Sonyflake) NextID() (uint64, error) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	return sf.nextIDInternal(time.Now())
}

// NextReproducibleID generates the next ID for a given timestamp.
// This API is required if you need to generate reproducible IDs.
// The IDs generated via this API are reproducible assuming that there
// is only 1 thread using the Sonyflake instance and that it is only calling
// NextReproducibleID and not NextID.
func (sf *Sonyflake) NextReproducibleID(now time.Time) (uint64, error) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	return sf.nextIDInternal(now)
}

// caller must hold sf.mutex
func (sf *Sonyflake) nextIDInternal(now time.Time) (uint64, error) {
	const maskSequence = uint16(1<<BitLenSequence - 1)

	current := currentElapsedTime(now, sf.startTime)
	if sf.elapsedTime < current {
		// clock has progressed to a new timestamp
		sf.elapsedTime = current
		sf.sequence = 0
	} else {
		// clock has not progressed since last check (could even have gone backwards)
		sf.sequence = (sf.sequence + 1) & maskSequence
		if sf.sequence == 0 {
			sf.elapsedTime++
			overtime := sf.elapsedTime - current
			time.Sleep(sleepTime(now, overtime))
		}
	}

	return sf.toID()
}

const sonyflakeTimeUnit = 1e7 // nsec, i.e. 10 msec

func toSonyflakeTime(t time.Time) int64 {
	return t.UTC().UnixNano() / sonyflakeTimeUnit
}

func currentElapsedTime(now time.Time, startTime int64) int64 {
	return toSonyflakeTime(now) - startTime
}

func sleepTime(now time.Time, overtime int64) time.Duration {
	return time.Duration(overtime)*10*time.Millisecond -
		time.Duration(now.UTC().UnixNano()%sonyflakeTimeUnit)*time.Nanosecond
}

func (sf *Sonyflake) toID() (uint64, error) {
	if sf.elapsedTime >= 1<<BitLenTime {
		return 0, errors.New("over the time limit")
	}

	return uint64(sf.elapsedTime)<<(BitLenSequence+BitLenMachineID) |
		uint64(sf.sequence)<<BitLenMachineID |
		uint64(sf.machineID), nil
}

func isPrivateIPv4(ip net.IP) bool {
	// See https://en.wikipedia.org/wiki/Reserved_IP_addresses
	return len(ip) == net.IPv4len &&
		ip[0] == 10 || // local communications within a private network.
		ip[0] == 100 && (ip[1] >= 64 && ip[1] < 128) ||
		ip[0] == 172 && (ip[1] >= 16 && ip[1] < 32) ||
		ip[0] == 192 && ip[1] == 168 || // ditto
		ip[0] == 198 && (ip[1] >= 18 && ip[1] < 20)
}

func isPrivateIPv6(ip net.IP) bool {
	// See https://en.wikipedia.org/wiki/Reserved_IP_addresses
	return len(ip) == net.IPv6len &&
		ip[0] == 0xfc || // fc00: unique local
		ip[0] == 254 && ip[1] == 128 // fe80: link local
}

// MachineID tries to compute a "semi unique" ID for this machine.
// Will return an error if it fails to inspect the network interfaces,
// or if it completely fails to find a usable ID.
func MachineID() (uint16, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return 0, fmt.Errorf("failed to get network interfaces: %w", err)
	}

	var ipNets []*net.IPNet // Collect IP nets on first pass, to avoid excessive syscalls

	// First try to find a private IPv4 address and use the lower 2 bytes as machine id
	for _, iface := range interfaces {
		addrs, addrErr := iface.Addrs()
		if addrErr != nil {
			continue
		}
		for _, addr := range addrs {
			ipnet, isIP := addr.(*net.IPNet)
			if !isIP || ipnet.IP.IsLoopback() {
				continue
			}

			ipNets = append(ipNets, ipnet)

			ip := ipnet.IP.To4()
			if ip != nil && isPrivateIPv4(ip) {
				return uint16(ip[2])<<8 + uint16(ip[3]), nil
			}
		}
	}

	// Second try to find a private IPv6 address and use the lower 2 bytes as machine id
	for _, ipnet := range ipNets {
		ip := ipnet.IP.To16()
		if ip != nil && isPrivateIPv6(ip) {
			return uint16(ip[net.IPv6len-2])<<8 + uint16(ip[net.IPv6len-1]), nil
		}
	}

	// last resort, try to base the machine id on the MAC address
	for _, iface := range interfaces {
		// Loopback interfaces sometimes have a nil hardware address.
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		if iface.HardwareAddr != nil {
			// Docker increments the MAC address by one for each container. Other
			// systems might do something completely different. To be safe, we hash
			// the hardware address and take the first two bytes.
			hash := sha256.New().Sum(iface.HardwareAddr)
			return uint16(hash[0])<<8 + uint16(hash[1]), nil
		}
	}

	return 0, ErrNoMachineID
}

// Decompose returns a set of Sonyflake ID parts.
// For optimal performance use DecomposeToBuffer instead.
func Decompose(id uint64) map[string]uint64 {
	buf := Buffer{}
	DecomposeToBuffer(id, &buf)
	return map[string]uint64{
		"id":         id,
		"msb":        uint64(buf.MSB),
		"time":       buf.Time,
		"sequence":   uint64(buf.Sequence),
		"machine-id": uint64(buf.MachineID),
	}
}

// DecomposeToBuffer writes the constituent parts of the Sonyflake ID
// to the provided buffer.
func DecomposeToBuffer(id uint64, buf *Buffer) {
	const maskSequence = uint64((1<<BitLenSequence - 1) << BitLenMachineID)
	const maskMachineID = uint64(1<<BitLenMachineID - 1)

	msb := id >> 63
	time := id >> (BitLenSequence + BitLenMachineID)
	sequence := id & maskSequence >> BitLenMachineID
	machineID := id & maskMachineID

	buf.Time = time
	buf.Sequence = uint8(sequence)
	buf.MSB = uint8(msb)
	buf.MachineID = uint16(machineID)
}
