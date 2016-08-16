package gossip

import (
	"io"
	"log"

	"github.com/influxdata/influxdb/influxql"
)

// RemoteFloatIterator foo
type RemoteFloatIterator struct {
	PointDecoder *influxql.PointDecoder
	Closed       bool
	CloseReader  func()
}

// Stats foo
func (rfi RemoteFloatIterator) Stats() influxql.IteratorStats {
	return rfi.PointDecoder.Stats()
}

// Close foo
func (rfi RemoteFloatIterator) Close() error {
	rfi.Closed = true
	rfi.CloseReader()
	return nil
}

// Next foo
func (rfi RemoteFloatIterator) Next() (*influxql.FloatPoint, error) {
	if rfi.Closed {
		return nil, nil
	}
	point, err := getPoint(rfi.PointDecoder)
	if err != nil || point == nil {
		rfi.CloseReader()
		return nil, err
	}
	return point.(*influxql.FloatPoint), nil
}

// RemoteIntegerIterator foo
type RemoteIntegerIterator struct {
	PointDecoder *influxql.PointDecoder
	Closed       bool
	CloseReader  func()
}

// Stats foo
func (rii RemoteIntegerIterator) Stats() influxql.IteratorStats {
	return rii.PointDecoder.Stats()
}

// Close foo
func (rii RemoteIntegerIterator) Close() error {
	rii.Closed = true
	rii.CloseReader()
	return nil
}

// Next foo
func (rii RemoteIntegerIterator) Next() (*influxql.IntegerPoint, error) {
	if rii.Closed {
		return nil, nil
	}
	point, err := getPoint(rii.PointDecoder)
	if err != nil || point == nil {
		rii.CloseReader()
		return nil, err
	}
	return point.(*influxql.IntegerPoint), nil
}

// RemoteStringIterator foo
type RemoteStringIterator struct {
	PointDecoder *influxql.PointDecoder
	Closed       bool
	CloseReader  func()
}

// Stats foo
func (rsi RemoteStringIterator) Stats() influxql.IteratorStats {
	return rsi.PointDecoder.Stats()
}

// Close foo
func (rsi RemoteStringIterator) Close() error {
	rsi.Closed = true
	rsi.CloseReader()
	return nil
}

// Next foo
func (rsi RemoteStringIterator) Next() (*influxql.StringPoint, error) {
	if rsi.Closed {
		return nil, nil
	}
	point, err := getPoint(rsi.PointDecoder)
	if err != nil || point == nil {
		rsi.CloseReader()
		return nil, err
	}
	return point.(*influxql.StringPoint), nil
}

// RemoteBooleanIterator foo
type RemoteBooleanIterator struct {
	PointDecoder *influxql.PointDecoder
	Closed       bool
	CloseReader  func()
}

// Stats foo
func (rbi RemoteBooleanIterator) Stats() influxql.IteratorStats {
	return rbi.PointDecoder.Stats()
}

// Close foo
func (rbi RemoteBooleanIterator) Close() error {
	rbi.Closed = true
	rbi.CloseReader()
	return nil
}

// Next foo
func (rbi RemoteBooleanIterator) Next() (*influxql.BooleanPoint, error) {
	if rbi.Closed {
		return nil, nil
	}
	point, err := getPoint(rbi.PointDecoder)
	if err != nil || point == nil {
		rbi.CloseReader()
		return nil, err
	}
	return point.(*influxql.BooleanPoint), nil
}

func getPoint(dec *influxql.PointDecoder) (influxql.Point, error) {
	var point influxql.Point
	err := dec.DecodePoint(&point)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		log.Printf("Error while decoding point: %s", err.Error())
		return nil, err
	}
	return point, nil
}
