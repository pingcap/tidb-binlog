package binlog

import (
	"path"
	"os"
	"io"
	"io/ioutil"
	"testing"

	"github.com/pingcap/tidb-binlog/binlog/scheme"
)

func TestCreate(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "binlogtest")
        if err != nil {
                t.Fatal(err)
        }
        defer os.RemoveAll(dir)

	b, err := Create(dir)
	if err != nil {
		t.Fatalf("err = %v, but want nil", err)
	}

	curFile := b.tail()
        curOffset, err := curFile.Seek(0, os.SEEK_CUR)
        if err != nil {
                t.Fatalf("err = %v, want nil", err)
        }

        fileInfo, err := curFile.Stat()
        if err != nil {
                t.Fatalf("err = %v, want nil", err)
        }

        size := fileInfo.Size()

        if curOffset != size {
                t.Errorf("offset = %d, but = %d", curOffset, size)
        }

        if g := path.Base(curFile.Name()); g != fileName(0) {
                t.Errorf("name = %+v, want %+v", g, fileName(1))
        }

	if len(b.locks) != 1 {
		t.Errorf("count of locks file = %d, but want 1", len(b.locks))
	}

        b.Close()

	_, err = Create(dir)
	if err != os.ErrExist {
		t.Errorf("err = %v, but want %v", err, os.ErrExist)
	}

	if err == nil {
		b.Close()
	}
}

func TestOpenForWrite(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "binlogtest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	
	f, err := os.Create(path.Join(dir, fileName(0)))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	b, err := OpenForWrite(dir)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	if g := path.Base(b.tail().Name()); g != fileName(0) {
		t.Errorf("name = %+v, want %+v", g, fileName(0))
	}
	
	b.Close()

	name := fileName(1)
	f, err = os.Create(path.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}

	n, err := f.Write([]byte("testfortest"))
        if err != nil {
                t.Errorf("err = %v, write len = %d, want", err, n)
        }
		
	f.Close()

	b, err = OpenForWrite(dir)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	curFile := b.tail()
	curOffset, err := curFile.Seek(0, os.SEEK_CUR)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	fileInfo, err := curFile.Stat()
	if err != nil {
                t.Fatalf("err = %v, want nil", err)
        }

	size := fileInfo.Size()

	if curOffset != size {
		t.Errorf("offset = %d, but = %d", curOffset, size)
	}

        if g := path.Base(curFile.Name()); g != fileName(1) {
                t.Errorf("name = %+v, want %+v", g, fileName(1))
        }

        b.Close()
}

func TestRotateFile(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "binlogtest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	b, err := Create(dir)
	if err != nil {
		t.Fatal(err)
	}

	ent := scheme.Entry {
		CommitTs:	1,
		StartTs:	0,
		Size:		10,
		Payload:	[]byte("binlogtest"),
	}

	if err = b.Write([]scheme.Entry{ent}); err != nil {
		t.Fatal(err)
	}

	if err = b.rotate(); err != nil {
		t.Fatal(err)
	}

	binlogName := fileName(1)
	if g := path.Base(b.tail().Name()); g != binlogName {
                t.Errorf("name = %+v, want %+v", g, binlogName)
        }

	if err = b.Write([]scheme.Entry{ent}); err != nil {
                t.Fatal(err)
        }

	b.Close()

	b, err = Open(dir, scheme.BinlogOffset{})
	if err != nil {
		t.Fatal(err)
	}

	f1, err := b.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()

	b, err = Open(dir, scheme.BinlogOffset{Suffix:1, Offset:0})
        if err != nil {
                t.Fatal(err)
        }

        f2, err := b.Read(1)
        if err != nil {
                t.Fatal(err)
        }
        b.Close()

	if len(f1) != 1 {
		t.Fatalf("nums of read entry = %d, but want 1", len(f1))
	}

	if len(f2) != 1 {
                t.Fatalf("nums of read entry = %d, but want 1", len(f2))
        }

	if f1[0].Offset.Suffix != 0 || f1[0].Offset.Offset != 0 {
		t.Fatalf("entry 1 offset is err, index = %d, offset = %d", f1[0].Offset.Suffix, f1[0].Offset.Offset)
	}

	if f2[0].Offset.Suffix != 1 || f2[0].Offset.Offset != 0 {
                t.Fatalf("entry 2 offset is err, index = %d, offset = %d", f2[0].Offset.Suffix, f2[0].Offset.Offset)
        }

	if f1[0].CommitTs != 1 || f1[0].StartTs  != 0 || string(f1[0].Payload) != "binlogtest" {
		t.Fatalf("entry 1  is err, commitTs = %d, startTs = %d, payload = %s", f1[0].CommitTs, f1[0].StartTs, string(f1[0].Payload))
	}
	if f2[0].CommitTs != 1 || f2[0].StartTs  != 0 || string(f2[0].Payload) != "binlogtest" {
                t.Fatalf("entry 2 is err, commitTs = %d, startTs = %d, payload = %s", f2[0].CommitTs, f2[0].StartTs, string(f2[0].Payload))
	}
}

func TestRead(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "binlogtest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	b, err := Create(dir)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()

	var es []scheme.Entry
	for i := 0; i < 10; i++ {
		es = append(es, scheme.Entry {
                        CommitTs:       1,
                        StartTs:        0,
                        Size:           10,
                        Payload:        []byte("binlogtest"),
                })
	}
	
	for i := 0; i < 20; i++ {
		b, err := OpenForWrite(dir)
        	if err != nil {
                	t.Fatal(err)
        	}
		if err = b.Write(es); err != nil {
			t.Fatal(err)
		}

		if i % 2 == 1 {
			if err = b.rotate(); err != nil {
				t.Fatal(err)
			}
		}
		b.Close()
	}

	b2, err := Open(dir, scheme.BinlogOffset{})
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	ents, err := b2.Read(11)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if ents[10].Offset.Suffix != 0  || ents[10].Offset.Offset != 480 {
		t.Errorf("last index read = %d, want %d; offset = %d, want %d", ents[10].Offset.Suffix, 0, ents[10].Offset.Offset, 480)
	}

	ents, err = b2.Read(11)
        if err != nil {
                t.Fatalf("err = %v, want nil", err)
        }
        if ents[10].Offset.Suffix != 1  || ents[10].Offset.Offset != 48 {
                t.Errorf("last index read = %d, want %d; offset = %d, want %d", ents[10].Offset.Suffix, 1, ents[10].Offset.Offset, 48)
        }

	ents, err = b2.Read(18)
        if err != nil {
                t.Fatalf("err = %v, want nil", err)
        }
        if ents[17].Offset.Suffix != 1  || ents[17].Offset.Offset !=  48*19{
                t.Errorf("last index read = %d, want %d; offset = %d, want %d", ents[17].Offset.Suffix, 1, ents[17].Offset.Offset, 48*19)
        }

	b3, err := Open(dir, scheme.BinlogOffset{Offset: 48, Suffix: 5})
        if err != nil {
                t.Fatal(err)
        }
        defer b3.Close()
        ents, err = b3.Read(20)
        if err != nil {
                t.Fatalf("err = %v, want nil", err)
        }
        if ents[19].Offset.Suffix != 6 || ents[19].Offset.Offset != 0 {
                t.Errorf("last index read = %d, want %d; offset = %d, want %d", ents[19].Offset.Suffix, 6, ents[19].Offset.Offset, 0)
        }
}

func TestCourruption(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "binlogtest")
        if err != nil {
                t.Fatal(err)
        }
        defer os.RemoveAll(dir)

        b, err := Create(dir)
        if err != nil {
                t.Fatal(err)
        }
        b.Close()

        var es []scheme.Entry
        for i := 0; i < 4; i++ {
                es = append(es, scheme.Entry {
                        CommitTs:       1,
                        StartTs:        0,
                        Size:           10,
                        Payload:        []byte("binlogtest"),
                })
        }

        for i := 0; i < 3; i++ {
                b, err := OpenForWrite(dir)
                if err != nil {
                        t.Fatal(err)
                }

                if err = b.Write(es); err != nil {
                        t.Fatal(err)
                }

                if err = b.rotate(); err != nil {
                	t.Fatal(err)
                }

                b.Close()
        }

	cfile1 := path.Join(dir, fileName(1))
	f1, err := os.OpenFile(cfile1, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}

	err = f1.Truncate(129)

	err = f1.Close()
	if err != nil {
		t.Fatal(err)
	}

	b1, err := Open(dir, scheme.BinlogOffset{Suffix:1, Offset:48})
        if err != nil {
                t.Fatal(err)
        }
        defer b1.Close()

        ents, err := b1.Read(4)
        if err != io.ErrUnexpectedEOF || len(ents) != 1 {
                t.Fatalf("err = %v, want nil; count of ent = %d, want 1", err, len(ents))
        }
}
