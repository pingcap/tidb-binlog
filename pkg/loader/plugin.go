package loader

type Plugin interface {
	DoFilter(*Txn) *Txn
}
