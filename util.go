package mgo

import (
	"context"
	"errors"
	"fmt"
	"github.com/yaziming/mgo/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func IsDup(err error) bool {
	switch raw := err.(type) {
	case mongo.BulkWriteError:
		return raw.WriteError.Code == 11000
	case mongo.WriteError:
		return raw.Code == 11000
	case *BulkError:
		for _, ecase := range raw.Cases() {
			if !IsDup(ecase.Err) {
				return false
			}
		}
		return true
	}
	return false
}

type indexKeyInfo struct {
	name    string
	key     bson.D
	weights bson.D
}

func ShouldErrorMatche(actual interface{}, params ...interface{}) string {
	err, ok := actual.(error)
	if !ok {
		return "Value is not an error"
	}
	_, errStr := matches(params[0], err.Error())
	return errStr
}
func ShouldMatch(actual interface{}, params ...interface{}) string {
	err, ok := actual.(string)
	if !ok {
		return "Value is not an string"
	}
	_, errStr := matches(params[0], err)
	return errStr
}
func matches(value, regex interface{}) (result bool, error string) {
	reStr, ok := regex.(string)
	if !ok {
		return false, "Regex must be a string"
	}
	valueStr, valueIsStr := value.(string)
	if !valueIsStr {
		if valueWithStr, valueHasStr := value.(fmt.Stringer); valueHasStr {
			valueStr, valueIsStr = valueWithStr.String(), true
		}
	}
	if valueIsStr {
		matches, err := regexp.MatchString("^"+reStr+"$", valueStr)
		if err != nil {
			return false, "Can't compile regex: " + err.Error()
		}
		return matches, ""
	}
	return false, "Obtained value is not a string and has no .String()"
}
func parseIndexKey(key []string) (*indexKeyInfo, error) {
	var keyInfo indexKeyInfo
	isText := false
	var order interface{}
	for _, field := range key {
		raw := field
		if keyInfo.name != "" {
			keyInfo.name += "_"
		}
		var kind string
		if field != "" {
			if field[0] == '$' {
				if c := strings.Index(field, ":"); c > 1 && c < len(field)-1 {
					kind = field[1:c]
					field = field[c+1:]
					keyInfo.name += field + "_" + kind
				} else {
					field = "\x00"
				}
			}
			switch field[0] {
			case 0:
				// Logic above failed. Reset and error.
				field = ""
			case '@':
				order = "2d"
				field = field[1:]
				// The shell used to render this field as key_ instead of key_2d,
				// and mgo followed suit. This has been fixed in recent server
				// releases, and mgo followed as well.
				keyInfo.name += field + "_2d"
			case '-':
				order = -1
				field = field[1:]
				keyInfo.name += field + "_-1"
			case '+':
				field = field[1:]
				fallthrough
			default:
				if kind == "" {
					order = 1
					keyInfo.name += field + "_1"
				} else {
					order = kind
				}
			}
		}
		if field == "" || kind != "" && order != kind {
			return nil, fmt.Errorf(`invalid index key: want "[$<kind>:][-]<field name>", got %q`, raw)
		}
		if kind == "text" {
			if !isText {
				keyInfo.key = append(keyInfo.key, bson.E{Key: "_fts", Value: "text"}, bson.E{Key: "_ftsx", Value: 1})
				isText = true
			}
			keyInfo.weights = append(keyInfo.weights, bson.E{Key: field, Value: 1})
		} else {
			keyInfo.key = append(keyInfo.key, bson.E{Key: field, Value: order})
		}
	}
	if keyInfo.name == "" {
		return nil, errors.New("invalid index key: no fields provided")
	}
	return &keyInfo, nil
}

func queryID(id interface{}) (realID interface{}) {
	switch newId := id.(type) {
	case string:
		var err error
		realID, err = bson.ObjectIDFromHex(newId)
		if err != nil {
			realID = id
		}
	case bson.ObjectId:
		realID = newId
	default:
		realID = id
	}
	return
}
func resolveAddr(addr string) (*net.TCPAddr, error) {
	// Simple cases that do not need actual resolution. Works with IPv4 and v6.
	if host, port, err := net.SplitHostPort(addr); err == nil {
		if port, _ := strconv.Atoi(port); port > 0 {
			zone := ""
			if i := strings.LastIndex(host, "%"); i >= 0 {
				zone = host[i+1:]
				host = host[:i]
			}
			ip := net.ParseIP(host)
			if ip != nil {
				return &net.TCPAddr{IP: ip, Port: port, Zone: zone}, nil
			}
		}
	}

	// Attempt to resolve IPv4 and v6 concurrently.
	addrChan := make(chan *net.TCPAddr, 2)
	for _, network := range []string{"udp4", "udp6"} {
		network := network
		go func() {
			// The unfortunate UDP dialing hack allows having a timeout on address resolution.
			conn, err := net.DialTimeout(network, addr, 10*time.Second)
			if err != nil {
				addrChan <- nil
			} else {
				addrChan <- (*net.TCPAddr)(conn.RemoteAddr().(*net.UDPAddr))
				conn.Close()
			}
		}()
	}

	// Wait for the result of IPv4 and v6 resolution. Use IPv4 if available.
	tcpaddr := <-addrChan
	if tcpaddr == nil || len(tcpaddr.IP) != 4 {
		ctx := context.TODO()
		if tcpaddr != nil {
			// Don't wait too long if an IPv6 address is known.
			ctx, _ = context.WithTimeout(ctx, 50*time.Millisecond)
		}
		select {
		case <-ctx.Done():
		case tcpaddr2 := <-addrChan:
			if tcpaddr == nil || tcpaddr2 != nil {
				// It's an IPv4 address or the only known address. Use it.
				tcpaddr = tcpaddr2
			}
		}
	}

	if tcpaddr == nil {
		//log("SYNC Failed to resolve server address: ", addr)
		return nil, errors.New("failed to resolve server address: " + addr)
	}
	if tcpaddr.String() != addr {
		//debug("SYNC Address ", addr, " resolved as ", tcpaddr.String())
	}
	return tcpaddr, nil
}
