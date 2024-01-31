package framework

import (
	"testing"
	"time"
)

func TestAmbiguousTime(t *testing.T) {
	ambTimeStr := "1991-09-14 23:00:00"
	t1, _ := time.ParseInLocation(TimeFormat, ambTimeStr, Local)
	if NotAmbiguousTime(t1) {
		t.Fatalf("%v should be a ambiguous time", t1)
	}
	t1Unix := t1.Unix()
	t2 := time.Unix(t1Unix, 0).In(Local)
	t2Str := t2.Format(TimeFormat)
	if t2Str != ambTimeStr {
		t.Fatalf("%s != %s", t2Str, ambTimeStr)
	}

	validTimeStr := "1991-09-14 22:00:00"
	t1, _ = time.ParseInLocation(TimeFormat, validTimeStr, Local)
	if !NotAmbiguousTime(t1) {
		t.Fatalf("%v should not be a ambiguous time", t1)
	}
	t1Unix = t1.Unix()
	t2 = time.Unix(t1Unix, 0).In(Local)
	t2Str = t2.Format(TimeFormat)
	if t2Str != validTimeStr {
		t.Fatalf("%s != %s", t2Str, validTimeStr)
	}
}

func TestTimeStampValue(t *testing.T) {
	t1Unix := MinTIMESTAMP.Unix()
	t2 := time.Unix(t1Unix, 0).In(Local)
	t2Str := t2.Format(TimeFormat)
	if t2Str != MINTIMESTAMP {
		t.Fatalf("%s != %s", t2Str, MINTIMESTAMP)
	}
}
