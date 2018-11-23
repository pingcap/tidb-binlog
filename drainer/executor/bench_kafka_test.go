package executor

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/ngaut/log"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

func BenchmarkKafka(b *testing.B) {
	log.SetLevelByString("error")

	cfg := &DBConfig{
		KafkaAddrs:   "127.0.0.1:9092",
		KafkaVersion: "0.8.2.0",
		ClusterID:    99900,
	}

	executor, err := newKafka(cfg)
	if err != nil {
		b.Fatal(err)
	}

	bytes := make([]byte, 128)
	table := &obinlog.Table{
		SchemaName: proto.String("test"),
		TableName:  proto.String("test"),
		ColumnInfo: []*obinlog.ColumnInfo{
			&obinlog.ColumnInfo{Name: "id", MysqlType: "int"},
			&obinlog.ColumnInfo{Name: "a1", MysqlType: "blob"},
		},
		Mutations: []*obinlog.TableMutation{
			&obinlog.TableMutation{
				Type: obinlog.MutationType_Insert.Enum(),
				Row: &obinlog.Row{
					Columns: []*obinlog.Column{
						&obinlog.Column{
							Int64Value: proto.Int64(1),
						},
						&obinlog.Column{
							BytesValue: bytes,
						},
					},
				},
			},
		},
	}

	b.ResetTimer()
	var arg = []interface{}{table}
	var args = [][]interface{}{arg}

	for i := 0; i < b.N; i++ {
		err = executor.Execute([]string{""}, args, []int64{int64(i)}, false)
		if err != nil {
			b.Fatal(err)
		}
	}

}
